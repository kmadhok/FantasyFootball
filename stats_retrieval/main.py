import asyncio
import argparse
import subprocess
import sys
from pathlib import Path
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
import pandas as pd

def ngs_stats_url(stat_type="rushing", year=2025, phase="REG", week=1, sort_hash="yards"):
    return f"https://nextgenstats.nfl.com/stats/{stat_type}/{year}/{phase}/{week}#{sort_hash}"

async def fetch_ngs_stats_table(stat_type="rushing", year=2025, week=1, phase="REG", sort_hash="yards"):
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
            f"location.pathname.endsWith('/stats/{stat_type}/{year}/{phase}/{week}') && "
            f"!!document.querySelector('table')"
        ),
        css_selector="table",
        scan_full_page=True,
        word_count_threshold=1,
    )

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        await crawler.arun(url="https://nextgenstats.nfl.com/", config=warm_cfg)
        res = await crawler.arun(url=ngs_stats_url(stat_type, year, phase, week, sort_hash), config=run_cfg)
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
    parser = argparse.ArgumentParser(description="Fetch NFL Next Gen Stats data")
    parser.add_argument("--stat-type", choices=["rushing", "receiving", "passing"], default="rushing", 
                        help="Type of stats to fetch (default: rushing)")
    parser.add_argument("--year", type=int, default=2025, help="Season year (default: 2025)")
    parser.add_argument("--week", type=int, default=1, help="Week number (default: 1)")
    parser.add_argument("--phase", default="REG", help="Season phase (default: REG)")
    parser.add_argument("--sort-hash", default="yards", help="Sort parameter (default: yards)")
    parser.add_argument("--fix-headers", action="store_true", default=True, help="Automatically fix CSV headers (default: True)")
    parser.add_argument("--no-fix-headers", dest="fix_headers", action="store_false", help="Skip automatic header fixing")
    
    args = parser.parse_args()
    
    df = asyncio.run(fetch_ngs_stats_table(
        stat_type=args.stat_type,
        year=args.year, 
        week=args.week, 
        phase=args.phase, 
        sort_hash=args.sort_hash
    ))
    print(df.head(10))
    
    # Include stat type in output filename
    output_filename = f"ngs_{args.stat_type}_week{args.week}_{args.year}.csv"
    df.to_csv(output_filename, index=False)
    print(f"Data saved to {output_filename}")
    
    # Automatically fix headers if enabled
    if args.fix_headers:
        print("\nFixing CSV headers...")
        script_dir = Path(__file__).parent
        fix_script = script_dir / "fix_column_headers.py"
        
        if fix_script.exists():
            try:
                result = subprocess.run([
                    sys.executable, str(fix_script),
                    "--input-file", output_filename,
                    "--stat-type", args.stat_type
                ], capture_output=True, text=True, cwd=script_dir)
                
                if result.returncode == 0:
                    print("✓ Headers fixed successfully")
                    if result.stdout:
                        # Print relevant output from fix script
                        lines = result.stdout.split('\n')
                        for line in lines:
                            if '✓' in line or 'Old:' in line or 'New:' in line:
                                print(f"  {line}")
                else:
                    print(f"⚠ Header fixing failed: {result.stderr}")
            except Exception as e:
                print(f"⚠ Error running header fix: {e}")
        else:
            print(f"⚠ Header fix script not found at {fix_script}")
