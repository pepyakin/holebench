#!/bin/sh

cargo run --release -- \
    --size 8G \
    --ratio 0.3 \
    --filename /mnt/hole_bench_1 \
    --backend io_uring --backlog 10000 --direct --num-jobs=4 --bs=4096
