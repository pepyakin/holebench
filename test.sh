#!/bin/sh

cargo run --release -- \
    --size 100G \
    --ratio 0.5 \
    --filename /mnt/hole_bench_1 \
    --backend mmap
