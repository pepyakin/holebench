#!/bin/sh

cargo run --release -- \
    --size 250G \
    --ratio 0.1 \
    --filename /mnt/hole_bench
