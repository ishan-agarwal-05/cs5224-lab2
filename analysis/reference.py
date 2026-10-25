"""Independent, standard-library oracle for verifying the distributed Spark output."""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

LINE = re.compile(r"^\s*([0-9]+)\s+([0-9]+)\s*$")


def parse_edge(line: str):
    match = LINE.fullmatch(line)
    if not match:
        return None
    return tuple(str(int(value)) for value in match.groups())


def analyse(path: Path):
    audit = Counter(raw_lines=0, blank_lines=0, malformed_lines=0,
                    self_loop_lines=0, valid_nonself_lines=0)
    edges = set()
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for raw in source:
            digest.update(raw)
            audit['raw_lines'] += 1
            line = raw.decode('utf-8', errors='replace')
            if not line.strip():
                audit['blank_lines'] += 1
                continue
            edge = parse_edge(line)
            if edge is None:
                audit['malformed_lines'] += 1
                continue
            if edge[0] == edge[1]:
                audit['self_loop_lines'] += 1
                continue
            audit['valid_nonself_lines'] += 1
            edges.add(edge)
    followers, followees = Counter(), Counter()
    for src, dst in edges:
        followees[src] += 1
        followers[dst] += 1
    users = sorted(followers.keys() | followees.keys(), key=int)
    rows = [dict(user_id=user, followers=followers[user], followees=followees[user])
            for user in users]
    audit.update(unique_edges=len(edges), unique_users=len(users),
                 duplicate_edges_removed=audit['valid_nonself_lines'] - len(edges),
                 total_followers=sum(followers.values()),
                 total_followees=sum(followees.values()))
    metadata = dict(audit)
    metadata.update(input_sha256=digest.hexdigest(), input_bytes=path.stat().st_size,
                    engine='Python independent verification (not Spark/cloud evidence)',
                    generated_at=datetime.now(timezone.utc).isoformat(),
                    cleaning_policy='Numeric directed edges; canonical IDs; drop blanks, malformed rows, self-links; deduplicate (source,target).')
    return rows, metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows, metadata = analyse(args.input)
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'users.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['user_id', 'followers', 'followees'])
        writer.writeheader()
        writer.writerows(rows)
    (args.output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(json.dumps(metadata, indent=2))
    print(json.dumps([r for r in rows if r['user_id'] in {'214328887','107830991'}], indent=2))


if __name__ == '__main__':
    main()
