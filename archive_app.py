#!/usr/bin/env python3
"""Reorganize rotated service logs into per-day gzip archives plus an index.

Source layout: <logs>/<service>_<YYYYMMDD>_<seq>.log. Rotation is size based,
so a file can span midnight and the tail of one day lands in a file named for
the next day; the last line before a rotation may be written into both the
old and the new file.

Rules:
  * Timestamps appear in three shapes:
      2026-09-07T08:08:39+08:00   explicit +08:00 offset
      2026-09-07T00:08:39+00:00   UTC, converted to Beijing time (+08:00)
      2026-09-07T08:08:39         bare time, machines are all in +08:00
    A line is bucketed by its Beijing-time calendar date, so UTC lines after
    16:00 roll into the next Beijing day.
  * A handful of lines have truncated timestamps. When the date part survives
    it is used directly; otherwise the day is filled from the nearest valid
    line in the same source file (previous, then next), falling back to the
    date encoded in the file name. Every such assignment is reported.
  * Lines duplicated by rotation (same service, identical raw bytes in two
    adjacent files) are kept once. Deduplication happens before bucketing, so
    index counts reflect what is actually inside the archives.
  * Output: <out>/<YYYY-MM-DD>.log.gz, one archive per Beijing day, and
    <out>/index.csv with one row per day:
        date,line_count,source_file_count,service_count
    line_count is the number of lines physically present in that gzip.

Run with an isolated interpreter when the input is untrusted:
    python3 -I archive_app.py --logs logs --out archive
"""

import argparse
import csv
import gzip
import os
import re
import sys
from datetime import datetime, timedelta, timezone

BJT = timezone(timedelta(hours=8))

# service_YYYYMMDD_seq.log
FILENAME_RE = re.compile(r"^(.+)_(\d{8})_(\d+)\.log$")

# Full timestamp, offset optional (bare time => Beijing local time).
TS_RE = re.compile(
    rb"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"
    rb"(?:([+-])(\d{2}):(\d{2}))?"
)
# Truncated timestamp whose date part is still complete: 2026-09-09T00: ...
DATE_PREFIX_RE = re.compile(rb"^(\d{4})-(\d{2})-(\d{2})T")


def parse_ts(line):
    """Return (beijing_date, utc_epoch_seconds) or (None, None) if broken."""
    m = TS_RE.match(line)
    if not m:
        return None, None
    year, mon, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    if m.group(7) is not None:
        sign = 1 if m.group(7) == b"+" else -1
        off = timezone(
            sign * timedelta(hours=int(m.group(8)), minutes=int(m.group(9)))
        )
        dt = datetime(year, mon, day, hour, minute, second, tzinfo=off)
    else:
        # Bare timestamp: the host is in Beijing time.
        dt = datetime(year, mon, day, hour, minute, second, tzinfo=BJT)
    bjt = dt.astimezone(BJT)
    return bjt.date(), int(dt.timestamp())


def salvage_date(line):
    """Date from a truncated-but-dated line such as '2026-09-09T00: ...'."""
    m = DATE_PREFIX_RE.match(line)
    if not m:
        return None
    from datetime import date

    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def collect(logs_dir):
    """Read every source file once.

    Returns records plus a warnings list. Each record is a dict; the sort key
    for broken-timestamp lines is copied from a neighboring valid line so the
    line keeps its original position inside its service's stream.
    """
    entries = []
    for name in os.listdir(logs_dir):
        m = FILENAME_RE.match(name)
        if not m:
            continue
        service, file_date, seq = m.group(1), m.group(2), int(m.group(3))
        entries.append((service, file_date, seq, name))
    # Deterministic global processing order: service, then rotation order.
    entries.sort(key=lambda e: (e[0], e[1], e[2]))

    records = []
    warnings = []
    seen_per_service = {}
    dup_dropped = 0

    for file_order, (service, file_date, _seq, name) in enumerate(entries):
        path = os.path.join(logs_dir, name)
        pending = []          # records awaiting a filled date / sort key
        last_day = None       # most recent good Beijing date in this file
        last_epoch = None
        with open(path, "rb") as fh:
            for line_no, raw in enumerate(fh, 1):
                if not raw.endswith(b"\n"):
                    raw += b"\n"

                # Rotation duplicates: identical bytes, same service.
                key = (service, raw)
                if key in seen_per_service:
                    dup_dropped += 1
                    warnings.append(
                        "duplicate dropped: %s:%d (already seen in %s)"
                        % (name, line_no, seen_per_service[key])
                    )
                    continue
                seen_per_service[key] = name

                day, epoch = parse_ts(raw)
                broken = day is None
                if broken:
                    day = salvage_date(raw)
                    if day is not None:
                        warnings.append(
                            "broken timestamp, date kept from prefix: "
                            "%s:%d -> %s" % (name, line_no, day.isoformat())
                        )
                    else:
                        warnings.append(
                            "broken timestamp, date inherited from neighbors: "
                            "%s:%d" % (name, line_no)
                        )
                else:
                    last_day, last_epoch = day, epoch
                pending.append(
                    {
                        "service": service,
                        "file": name,
                        "file_order": file_order,
                        "line_no": line_no,
                        "raw": raw,
                        "day": day,
                        "epoch": epoch,          # None for broken lines
                        "broken": broken,
                    }
                )

        # Fill forward from the nearest preceding valid line, then backward
        # from the next valid line, then fall back to the file-name date.
        fill_day, fill_epoch = None, None
        for rec in pending:
            if not rec["broken"]:
                fill_day, fill_epoch = rec["day"], rec["epoch"]
            elif fill_day is not None:
                rec["day"], rec["epoch"] = fill_day, fill_epoch
        fill_day, fill_epoch = None, None
        for rec in reversed(pending):
            if not rec["broken"]:
                fill_day, fill_epoch = rec["day"], rec["epoch"]
            elif rec["day"] is None and fill_day is not None:
                rec["day"], rec["epoch"] = fill_day, fill_epoch
        if any(rec["day"] is None for rec in pending):
            fallback = datetime.strptime(file_date, "%Y%m%d").date()
            fallback_epoch = int(
                datetime(
                    fallback.year, fallback.month, fallback.day, tzinfo=BJT
                ).timestamp()
            )
            for rec in pending:
                if rec["day"] is None:
                    rec["day"], rec["epoch"] = fallback, fallback_epoch
                    warnings.append(
                        "broken timestamp, date from file name: %s:%d -> %s"
                        % (rec["file"], rec["line_no"],
                           fallback.isoformat())
                    )
        records.extend(pending)

    return records, warnings, dup_dropped


def write_archives(records, out_dir):
    buckets = {}
    for rec in records:
        # Broken lines inherit the epoch of their neighboring valid line;
        # the `broken` tie-break keeps one immediately after that neighbor
        # instead of jumping ahead of it.
        sort_key = (
            rec["epoch"],
            1 if rec["broken"] else 0,
            rec["service"],
            rec["file_order"],
            rec["line_no"],
        )
        bucket = buckets.setdefault(
            rec["day"], {"lines": [], "files": set(), "services": set()}
        )
        bucket["lines"].append((sort_key, rec["raw"]))
        bucket["files"].add(rec["file"])
        bucket["services"].add(rec["service"])

    os.makedirs(out_dir, exist_ok=True)
    index_rows = []
    for day in sorted(buckets):
        bucket = buckets[day]
        bucket["lines"].sort(key=lambda item: item[0])
        gz_name = day.strftime("%Y-%m-%d") + ".log.gz"
        # mtime=0 and a fixed inner name make reruns byte-identical.
        with gzip.GzipFile(
            filename=day.strftime("%Y-%m-%d") + ".log",
            mode="wb",
            mtime=0,
            compresslevel=9,
            fileobj=open(os.path.join(out_dir, gz_name), "wb"),
        ) as gz:
            for _key, raw in bucket["lines"]:
                gz.write(raw)
        index_rows.append(
            {
                "date": day.strftime("%Y-%m-%d"),
                "line_count": len(bucket["lines"]),
                "source_file_count": len(bucket["files"]),
                "service_count": len(bucket["services"]),
            }
        )

    index_path = os.path.join(out_dir, "index.csv")
    with open(index_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "date", "line_count", "source_file_count", "service_count",
            ],
        )
        writer.writeheader()
        writer.writerows(index_rows)
    return index_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--logs", default="logs", help="source log directory")
    ap.add_argument(
        "--out", default="archive", help="output directory for gz and index"
    )
    args = ap.parse_args()

    if not os.path.isdir(args.logs):
        ap.error("logs directory not found: %s" % args.logs)

    records, warnings, dup_dropped = collect(args.logs)
    for w in warnings:
        print("warning: " + w, file=sys.stderr)

    index_rows = write_archives(records, args.out)
    total = sum(r["line_count"] for r in index_rows)
    print(
        "archived %d lines into %d day files; %d rotation duplicates removed"
        % (total, len(index_rows), dup_dropped)
    )
    print("index: %s" % os.path.join(args.out, "index.csv"))


if __name__ == "__main__":
    main()
