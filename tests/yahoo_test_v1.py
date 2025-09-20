
import asyncio, json, pandas as pd
from urllib.parse import quote
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

def yahoo_week_url(week:int=1, selected_table:int=0, season_phase:str="REGULAR_SEASON") -> str:
    wk = quote(json.dumps({"week": week, "seasonPhase": season_phase}))
    return f"https://sports.yahoo.com/nfl/stats/weekly/?selectedTable={selected_table}&week={wk}"

async def fetch_week_table(week=1, selected_table=0):
    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        java_script_enabled=True,
        # enable_stealth=True,            # if you hit bot checks
        # user_agent_mode="random",
        # text_mode=True,
    )

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=120_000,                 # give the page time to hydrate
        wait_until="networkidle",             # wait for JS/network to settle
        # ⬇️ Wait until the page visibly says WEEK {week} (case-insensitive)
        wait_for=f"js:() => new RegExp('\\\\bWEEK\\\\s+{week}\\\\b','i').test(document.body.innerText)",
        css_selector="table",                 # keep only tables in cleaned_html
        word_count_threshold=1,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        res = await crawler.arun(url=yahoo_week_url(week, selected_table), config=run_cfg)
        if not res.success:
            raise RuntimeError(res.error_message)

        tables = pd.read_html(res.cleaned_html)
        if not tables:
            raise ValueError("No tables found.")
        return tables[0]
# Run it
df = asyncio.run(fetch_week_table(week=2, selected_table=0))
print(df.head())

# Save if you want
df.to_csv("yahoo_week1_passing.csv", index=False)