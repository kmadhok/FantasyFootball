# run_both.py
import sys
import subprocess

def run(script, *args):
    """Run a Python script with this interpreter; raise if it fails."""
    cmd = [sys.executable, script, *map(str, args)]
    print(f"→ Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)   # inherits stdout/stderr

if __name__ == "__main__":
    # 1) Run abc.py (add args after script name if needed)
    run("passing_extraction.py")          # e.g., run("abc.py", "--week", 3)

    # 2) Only runs if abc.py succeeded (return code 0)
    run("fix_csv_headers.py")          # e.g., run("bcd.py", "--phase", "REG")

    print("✅ All done.")
