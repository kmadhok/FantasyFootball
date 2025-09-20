
import asyncio, json, pandas as pd
from urllib.parse import quote
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

def yahoo_week_url(week:int=2, selected_table:int=0, season_phase:str="REGULAR_SEASON") -> str:
    # selected_table: 0=first stats tab; increment for others on the page UI
    week_param = quote(json.dumps({"week": week, "seasonPhase": season_phase}))
    return f"https://sports.yahoo.com/nfl/stats/weekly/?selectedTable={selected_table}&week={week_param}"

async def fetch_week_table(week=3, selected_table=0):
    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        java_script_enabled=True,
        # Helpful if bot checks get in the way:
        # enable_stealth=True,
        # user_agent_mode="random",
        # text_mode=True,  # speed-up by disabling images
    )
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=90_000,         # ms
        wait_for="css:table",        # wait until a <table> is present
        css_selector="table",        # keep only tables in cleaned_html
        word_count_threshold=1,
        # scan_full_page=True, scroll_delay=0.3,  # uncomment if content loads on scroll
        verbose=False
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        res = await crawler.arun(url=yahoo_week_url(week, selected_table), config=run_cfg)
        if not res.success:
            raise RuntimeError(res.error_message)

        # Parse the rendered table HTML to DataFrame(s)
        tables = pd.read_html(res.cleaned_html)  # list[DataFrame]
        if not tables:
            raise ValueError("No tables found in page.")
        return tables[0]  # first (main) table

# Run it
df = asyncio.run(fetch_week_table(week=2, selected_table=0))
print(df.head())

# Save if you want
df.to_csv("yahoo_week3_passing.csv", index=False)