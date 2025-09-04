import requests

LEAGUE_ID = "1124820373402046464"          # ← put your league ID here


def get_rosters_from_league():
    url = f"https://api.sleeper.app/v1/league/{LEAGUE_ID}/rosters"
    response = requests.get(url, timeout=10)
    print(response.status_code)
    response.raise_for_status()           # blows up if API returns 4xx/5xx
    data = response.json()


    return data




def main() -> None:
    url = f"https://api.sleeper.app/v1/league/{LEAGUE_ID}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()           # blows up if API returns 4xx/5xx
    data = response.json()
    print(f'Hello, Sleeper!  League name: "{data["name"]}"')

if __name__ == "__main__":
    # main()
    data=get_rosters_from_league()
    print(data)