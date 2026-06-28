#!/usr/bin/env python3
"""
generate_data.py — Generate test files for the DDS project.

Usage:
  python3 generate_data.py words   # word-count test file
  python3 generate_data.py numbers # sum-numbers test file
  python3 generate_data.py lines   # line-count test file
"""
import random
import sys
import os

os.makedirs("data", exist_ok=True)

mode = sys.argv[1] if len(sys.argv) > 1 else "words"

if mode == "words":
    vocab = ["hello", "world", "distributed", "systems", "network",
             "coordinator", "worker", "chunk", "result", "aggregate",
             "epoll", "socket", "udp", "protocol", "process"]
    lines = [" ".join(random.choices(vocab, k=random.randint(8, 20)))
             for _ in range(500)]
    out = "data/words.txt"
    expected = sum(len(l.split()) for l in lines)
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Generated {out}  ({len(lines)} lines)")
    print(f"Expected word count: {expected}")

elif mode == "numbers":
    nums = [random.randint(-1000, 1000) for _ in range(2000)]
    out = "data/numbers.txt"
    with open(out, "w") as f:
        for i, n in enumerate(nums):
            end = "\n" if (i + 1) % 10 == 0 else " "
            f.write(str(n) + end)
    print(f"Generated {out}  ({len(nums)} numbers)")
    print(f"Expected sum: {sum(nums)}")

elif mode == "lines":
    lines = [f"Line number {i}: " + "x" * random.randint(10, 80)
             for i in range(1000)]
    out = "data/lines.txt"
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Generated {out}  ({len(lines)} lines)")
    print(f"Expected line count: {len(lines)}")

else:
    print(f"Unknown mode '{mode}'. Use: words | numbers | lines")
    sys.exit(1)
