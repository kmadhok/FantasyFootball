import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
import pandas as pd

def ngs_passing_url(year=2025, phase="REG", week=1, sort_hash="yards"):
    return f"https://nextgenstats.nfl.com/stats/rushing/{year}/{phase}/{week}#{sort_hash}"

async def fetch_ngs_passing_table(year=2025, week=1, phase="REG", sort_hash="yards"):
    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        java_script_enabled=True,
        # enable_stealth=True,  # uncomment if you hit bot checks
        # user_agent_mode="random",
    )

    warm_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=60_000,
        wait_until="domcontentloaded",
        wait_for="js:() => !!document.body",
        js_code=(
            "() => {"
            "  const sel=['#onetrust-accept-btn-handler','button#onetrust-accept-btn-handler',"
            "             'button[aria-label=\"I Accept\"]','#truste-consent-button'];"
            "  for (const s of sel){const b=document.querySelector(s); if(b){b.click(); break;}}"
            "}"
        ),
    )

    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=120_000,
        wait_until="domcontentloaded",
        wait_for=(
            f"js:() => "
            f"location.pathname.endsWith('/stats/passing/{year}/{phase}/{week}') && "
            f"!!document.querySelector('table')"
        ),
        css_selector="table",
        scan_full_page=True,
        word_count_threshold=1,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        await crawler.arun(url="https://nextgenstats.nfl.com/", config=warm_cfg)
        res = await crawler.arun(url=ngs_passing_url(year, phase, week, sort_hash), config=run_cfg)
        if not res.success:
            raise RuntimeError(res.error_message)

        dfs = pd.read_html(res.cleaned_html)  # requires lxml
        if not dfs:
            raise ValueError("No tables found after render.")
        df = max(dfs, key=lambda d: (len(d.index) * max(1, len(d.columns))))
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]
        return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract NFL rushing stats")
    parser.add_argument("--year", type=int, default=2025, help="NFL season year")
    parser.add_argument("--week", type=int, default=3, help="NFL week number")
    parser.add_argument("--phase", default="REG", help="Season phase (REG, PRE, POST)")
    parser.add_argument("--sort_hash", default="yards", help="Sort column")
    parser.add_argument("--output", default=None, help="Output CSV filename")
    args = parser.parse_args()
    
    # Generate default filename if not provided
    if args.output is None:
        args.output = f"ngs_rushing_{args.year}_wk{args.week}.csv"
    
    df = asyncio.run(fetch_ngs_passing_table(year=args.year, week=args.week, phase=args.phase, sort_hash=args.sort_hash))
    print(df.head(10))
    df.to_csv(args.output, index=False)
    print(f"Data saved to: {args.output}")
