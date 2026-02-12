git clone https://github.com/gkverghe/hedge-fund-tracker.git
cd hedge-fund-tracker
# Copy the script content
cp /path/to/qqq-stock-tracker/scripts/aggregate_qqq.py .github/scripts/aggregate_qqq.py
git add .github/scripts/aggregate_qqq.py
git commit -m "Add QQQ aggregation script"
git push
