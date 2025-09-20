import asyncio
import argparse
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
import pandas as pd

VALID_TYPES = {"passing", "rushing", "receiving"}

def ngs_url(stat_type: str, year: int = 2025, phase: str = "REG", week: int = 1, sort_hash: str = "yards") -> str:
    """Build Next Gen Stats URL for the chosen stat type."""
    if stat_type not in VALID_TYPES:
        raise ValueError(f"stat_type must be one of {sorted(VALID_TYPES)}")
    return f"https://nextgenstats.nfl.com/stats/{stat_type}/{year}/{phase}/{week}#{sort_hash}"

async def fetch_ngs_table(
    stat_type: str = "passing",
    year: int = 2025,
    week: int = 1,
    phase: str = "REG",
    sort_hash: str = "yards",
) -> pd.DataFrame:
    """Render the NGS table for the given stat type and return it as a DataFrame."""
    if stat_type not in VALID_TYPES:
        raise ValueError(f"stat_type must be one of {sorted(VALID_TYPES)}")

    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        java_script_enabled=True,
        # enable_stealth=True,  # uncomment if you hit bot checks
        # user_agent_mode="random",
    )

    # Warm/open once to clear consent banners etc.
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

    # Actual stats page render
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=120_000,
        wait_until="domcontentloaded",
        wait_for=(
            "js:() => ("
            f"  location.pathname.endsWith('/stats/{stat_type}/{year}/{phase}/{week}') && "
            "  !!document.querySelector('table')"
            ")"
        ),
        css_selector="table",
        scan_full_page=True,
        word_count_threshold=1,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # Open homepage to clear consent banner
        await crawler.arun(url="https://nextgenstats.nfl.com/", config=warm_cfg)

        # Navigate to the desired stats page
        url = ngs_url(stat_type, year, phase, week, sort_hash)
        res = await crawler.arun(url=url, config=run_cfg)
        if not res.success:
            raise RuntimeError(res.error_message)

        dfs = pd.read_html(res.cleaned_html)  # requires lxml
        if not dfs:
            raise ValueError("No tables found after render.")

        # Pick the largest table (rows * cols) and clean up
        df = max(dfs, key=lambda d: (len(d.index) * max(1, len(d.columns))))
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~pd.Index(df.columns).astype(str).str.match(r"^Unnamed")]
        return df

def parse_args():
    parser = argparse.ArgumentParser(description="Scrape NFL Next Gen Stats tables via Crawl4AI.")
    parser.add_argument("--type", dest="stat_type", choices=sorted(VALID_TYPES), default="passing",
                        help="Which stats page to fetch.")
    parser.add_argument("--year", type=int, default=2025, help="Season year.")
    parser.add_argument("--phase", choices=["PRE", "REG", "POST"], default="REG", help="Season phase.")
    parser.add_argument("--week", type=int, default=1, help="Week number.")
    parser.add_argument("--sort", dest="sort_hash", default="yards",
                        help="Hash anchor used by the site for default sorting (e.g., 'yards', 'td', etc.).")
    parser.add_argument("--outfile", default="scrape_test.csv", help="Where to write the CSV.")
    parser.add_argument("--head", type=int, default=10, help="Print first N rows to stdout.")
    return parser.parse_args()

async def amain():
    args = parse_args()
    df = await fetch_ngs_table(
        stat_type=args.stat_type,
        year=args.year,
        week=args.week,
        phase=args.phase,
        sort_hash=args.sort_hash,
    )
    print(df.head(args.head))
    df.to_csv(args.outfile, index=False)
    print(f"\nWrote {len(df)} rows to {args.outfile}")

if __name__ == "__main__":
    asyncio.run(amain())
