#!/bin/bash
echo "Processing Token Info"
node scripts/extract_tx.js 2>&1

echo "Calculating Profit"
python scripts/analysis_trades.py 2>&1