import requests

# --- CONFIG ------------------------------------------------------------------
SEASON     = 2025            # the season you want data for
LEAGUE_ID  = "73756"        # <— replace with your MFL league ID
# -----------------------------------------------------------------------------

def main() -> None:
    base = f"https://api.myfantasyleague.com/{SEASON}/export"
    params = {
        "TYPE": "league",    # endpoint that returns general league info
        "L":    LEAGUE_ID,   # league identifier
        "JSON": "1"          # ask for JSON instead of XML
    }

    r = requests.get(base, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    # MFL wraps most responses in a top‑level key named after the TYPE
    league_obj = data["league"]
    print(f'Hello, MFL!  League name: "{league_obj["name"]}"')

if __name__ == "__main__":
    main()
