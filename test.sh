#!/bin/sh

cargo run --release -- \
    --size 10G \
    --ratio 0.1 \
    --filename /mnt/hole_bench
    --ramp_time=2
