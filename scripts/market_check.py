#!/usr/bin/env python3
import datetime, sys, os, zoneinfo

# Hardcoded NSE holidays (dd-mmm-yy format → parsed into dates)
HOLIDAYS = [
    "19-Feb-25", "26-Feb-25", "14-Mar-25", "31-Mar-25",
    "01-Apr-25", "10-Apr-25", "14-Apr-25", "18-Apr-25",
    "01-May-25", "12-May-25", "05-Sep-25", "08-Sep-25",
    "15-Aug-25", "27-Aug-25", "02-Oct-25", "21-Oct-25",
    "22-Oct-25", "05-Nov-25", "25-Dec-25"
]


# Convert to datetime.date objects for current year
HOLIDAY_DATES = set(
    datetime.datetime.strptime(h, "%d-%b-%y").date()
    for h in HOLIDAYS
)

def is_market_open_day():
    tz = zoneinfo.ZoneInfo(os.environ.get("TIMEZONE", "Asia/Kolkata"))
    now = datetime.datetime.now(tz)

    # Weekend check
    if now.weekday() >= 5:  # 5=Saturday, 6=Sunday
        print("Weekend detected, exiting.")
        return False

    # NSE holidays check
    if now.date() in HOLIDAY_DATES:
        print(f"Holiday detected: {now.strftime('%d-%b-%y')}, exiting.")
        return False

    # Market hours check
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (market_open <= now <= market_close):
        print("Market is closed (outside hours).")
        return False

    print("Market is open.")
    return True

if __name__ == "__main__":
    market_open = is_market_open_day()
    
    # Write output for GitHub Actions
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write(f"market_open={str(market_open).lower()}\n")
