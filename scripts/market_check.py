#!/usr/bin/env python3
import datetime, sys, os, zoneinfo
import holidays

def is_market_open_day():
    tz = zoneinfo.ZoneInfo(os.environ.get("TIMEZONE", "Asia/Kolkata"))
    now = datetime.datetime.now(tz)
    ind_holidays = holidays.India(years=now.year)

    # Weekend check
    if now.weekday() >= 5:  # 5=Saturday, 6=Sunday
        print("Weekend detected, exiting.")
        return False

    # # NSE holidays
    # if now.date() in ind_holidays:
        # print(f"Holiday detected: {ind_holidays.get(now.date())}, exiting.")
        # return False

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
