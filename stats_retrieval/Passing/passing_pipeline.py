# run_both.py
import sys
import subprocess

def run(script, *args):
    """Run a Python script with this interpreter; raise if it fails."""
    cmd = [sys.executable, script, *map(str, args)]
    print(f"→ Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)   # inherits stdout/stderr

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run passing data pipeline")
    parser.add_argument("--year", type=int, default=2025, help="NFL season year")
    parser.add_argument("--week", type=int, default=3, help="NFL week number")
    parser.add_argument("--phase", default="REG", help="Season phase (REG, PRE, POST)")
    parser.add_argument("--sort_hash", default="yards", help="Sort column")
    parser.add_argument("--output", default=None, help="Output CSV filename")
    args = parser.parse_args()
    
    # Generate default filename if not provided
    if args.output is None:
        args.output = f"ngs_passing_{args.year}_wk{args.week}.csv"
    
    # 1) Run passing_extraction.py with parameters
    run("passing_extraction.py", "--year", args.year, "--week", args.week, "--phase", args.phase, "--sort_hash", args.sort_hash, "--output", args.output)

    # 2) Only runs if extraction succeeded (return code 0)
    run("fix_csv_headers.py", "--input", args.output)

    print("✅ All done.")
