#!/bin/sh

cargo run --release -- \
    --size 10G \
    --ratio 0.5 \
    --filename /mnt/hole_bench_1 \
    --no-sparse
