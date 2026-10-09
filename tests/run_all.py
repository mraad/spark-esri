"""Run the headless smoke tests, one subprocess each, and print a PASS/SKIP/FAIL table.

    python tests/run_all.py                 # run everything
    python tests/run_all.py t_qr            # run a subset (substring match)
    SPARK_ESRI_TEST_ROWS=1000000 python tests/run_all.py

Exits non-zero if any test FAILs. SKIPs (missing optional dependency) do not fail the run.

Each test needs a Spark that can run executor side python. On Windows with Python 3.12+
that means Spark >= 4.1.2 (SPARK-53759), which ArcGIS Pro does not yet bundle:

    pip install "pyspark>=4.1.2"
    set SPARK_HOME=%CONDA_PREFIX%\\Lib\\site-packages\\pyspark
"""
import os
import subprocess
import sys
import time

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
SKIP_EXIT_CODE = 77


def discover(patterns):
    names = sorted(f for f in os.listdir(TESTS_DIR)
                   if f.startswith("t_") and f.endswith(".py"))
    if patterns:
        names = [n for n in names if any(p in n for p in patterns)]
    return names


def main(argv):
    scripts = discover(argv)
    if not scripts:
        print("no matching tests", file=sys.stderr)
        return 2

    env = dict(os.environ)
    # Let each test import _harness, and spark_esri straight from the checkout.
    env["PYTHONPATH"] = os.pathsep.join(
        [TESTS_DIR, os.path.join(REPO_ROOT, "python"), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)

    print(f"SPARK_HOME = {env.get('SPARK_HOME', '(unset - pip pyspark in this env if any, else the Spark bundled with Pro)')}")
    print(f"rows       = {env.get('SPARK_ESRI_TEST_ROWS', '50000')}\n")

    results = []
    for script in scripts:
        label = script[:-3]
        print(f"{'=' * 70}\n>>> {label}\n{'=' * 70}", flush=True)
        started = time.time()
        proc = subprocess.run([sys.executable, os.path.join(TESTS_DIR, script)],
                              cwd=REPO_ROOT, env=env)
        elapsed = time.time() - started
        if proc.returncode == 0:
            status = "PASS"
        elif proc.returncode == SKIP_EXIT_CODE:
            status = "SKIP"
        else:
            status = "FAIL"
        results.append((label, status, elapsed))
        print(f"<<< {label}: {status} ({elapsed:.1f}s)\n", flush=True)

    width = max(len(name) for name, _, _ in results)
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for name, status, elapsed in results:
        print(f"  {status:4}  {name:<{width}}  {elapsed:6.1f}s")
    counts = {s: sum(1 for _, st, _ in results if st == s) for s in ("PASS", "SKIP", "FAIL")}
    print(f"\n{counts['PASS']} passed, {counts['SKIP']} skipped, {counts['FAIL']} failed")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
