from nfl_data_py import import_schedules
import pandas as pd

YEAR = 2025

try:
    df = import_schedules([YEAR])
    has_2025 = isinstance(df, pd.DataFrame) and not df.empty
    print(f"Has schedules for {YEAR}? {'YES ✅' if has_2025 else 'NO ❌'}")
    if has_2025:
        # Show a quick peek so you can confirm
        print(df[['season','week','game_id','home_team','away_team']].head())
except Exception as e:
    print("Error while checking schedules:", e)