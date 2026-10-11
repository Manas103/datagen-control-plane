#!/usr/bin/env python3
"""Generates a deterministic CSV of SubmitRequest rows for the bench and
replay-digest harnesses. Columns: request_id,tenant_id,priority,payload_kind.

Usage:
  gen_requests.py --count 50000 --seed 1 --out canonical.csv
  gen_requests.py --from canonical.csv --dup-pct 5 --shuffle --seed 2 --out noisy.csv
"""
import argparse
import csv
import random


def generate(count: int, seed: int):
    rng = random.Random(seed)
    rows = []
    for i in range(count):
        request_id = f"req-{seed}-{i:07d}"
        tenant_id = rng.randrange(0, 8)
        priority = rng.randrange(1, 6)
        payload_kind = rng.choice(["ingest_scan", "frame_mine", "replay_build"])
        rows.append((request_id, tenant_id, priority, payload_kind))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--from", dest="from_file", default=None)
    ap.add_argument("--dup-pct", type=float, default=0.0)
    ap.add_argument("--shuffle", action="store_true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if args.from_file:
        with open(args.from_file, newline="") as f:
            rows = [tuple(r) for r in csv.reader(f)]
    else:
        rows = generate(args.count, args.seed)

    rng = random.Random(args.seed + 1000)
    if args.dup_pct > 0:
        n_dup = int(len(rows) * args.dup_pct / 100.0)
        dups = rng.sample(rows, n_dup)
        rows = rows + dups

    if args.shuffle:
        rng.shuffle(rows)

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerows(rows)

    print(f"wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
