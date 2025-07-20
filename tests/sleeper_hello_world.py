import requests

LEAGUE_ID = "1124820373402046464"          # ← put your league ID here

def main() -> None:
    url = f"https://api.sleeper.app/v1/league/{LEAGUE_ID}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()           # blows up if API returns 4xx/5xx
    data = response.json()
    print(f'Hello, Sleeper!  League name: "{data["name"]}"')

if __name__ == "__main__":
    main()
