#!/bin/sh

cargo run --release -- \
    --size 48G \
    --ratio 1 \
    --filename /mnt/hole_bench_1 \
    --backend iouring --backlog 100
