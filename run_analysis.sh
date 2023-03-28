#!/usr/bin/env bash

EVENT_LOG=$1
OUTPUT_DIR=$2

poetry run wta --log_path "$EVENT_LOG" --output_dir "$OUTPUT_DIR" >> "$OUTPUT_DIR"/out.log
