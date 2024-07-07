#!/bin/bash
cd "$(dirname "$0")/../"
export PYTHONPATH=$PWD

python3 node/btc-vout-hashtable-builder/deal_block.py --start "$START" --end "$END"

