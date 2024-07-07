#!/bin/bash
cd "$(dirname "$0")/../"
export PYTHONPATH=$(pwd)
python3 node/btc-vout-hashtable-builder/deal_block.py

