#!/usr/bin/env python3
import re
import glob

experiments = {
    '1000': 20,   # avg watchers from output
    '5000': 102,
    '10000': 204,
    '30000': 612,
    '50000': 1020,
    '100000': 1866
}

print("| Experiment | Avg Watchers | READ_COUNT (P50) | READ_WATCHER_IDS (P50) | POST_NOTIFY (P50) | WAIT_NOTIFY (P50) | Total E2E (P50) |")
print("|------------|--------------|-------------------|------------------------|-------------------|-------------------|-----------------|")

for ops, watchers in sorted(experiments.items(), key=lambda x: int(x[0])):
    files = glob.glob(f'/tmp/2026_03_30_*_{ops}.txt')
    if files:
        with open(files[0], 'r') as f:
            content = f.read()

        # Find first client's breakdown
        match_read_count = re.search(r'1\. READ_COUNT.*?P50:\s+(\d+\.\d+)\s+μs', content, re.DOTALL)
        match_read_ids = re.search(r'2\. READ_WATCHER_IDS.*?P50:\s+(\d+\.\d+)\s+μs', content, re.DOTALL)
        match_post = re.search(r'3\. POST_NOTIFY.*?P50:\s+(\d+\.\d+)\s+μs', content, re.DOTALL)
        match_wait = re.search(r'4\. WAIT_NOTIFY.*?P50:\s+(\d+\.\d+)\s+μs', content, re.DOTALL)

        # Get E2E from main output
        match_e2e = re.search(r'NOTIFICATION PHASE \(\d+ ops\):\s+P50:\s+(\d+\.\d+)\s+μs', content)

        read_count = float(match_read_count.group(1)) if match_read_count else 0
        read_ids = float(match_read_ids.group(1)) if match_read_ids else 0
        post = float(match_post.group(1)) if match_post else 0
        wait = float(match_wait.group(1)) if match_wait else 0
        e2e = float(match_e2e.group(1)) if match_e2e else 0

        print(f"| {ops:>6} ops | {watchers:>4} | {read_count:>8.2f} μs | {read_ids:>10.2f} μs | {post:>8.2f} μs | {wait:>8.2f} μs | {e2e:>10.2f} μs |")
