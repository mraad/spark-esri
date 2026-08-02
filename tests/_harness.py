"""Shared helpers for the headless smoke tests.

These are plain scripts, not pytest - each one is executed in its own subprocess by
run_all.py, because a Spark session (and the pyspark import that backs it) is a
process-global singleton that cannot be torn down and rebuilt cleanly in-process.

Exit code convention (autotools style):
    0   PASS
    77  SKIP - an optional third party dependency is missing
    *   FAIL
"""
import importlib
import os
import sys

SKIP_EXIT_CODE = 77

# Row counts are scaled down from the notebooks so the suite stays a smoke test.
# Override with e.g. SPARK_ESRI_TEST_ROWS=1000000 to run at notebook scale.
ROWS = int(os.environ.get("SPARK_ESRI_TEST_ROWS", "50000"))

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def repo_root() -> str:
    """Absolute path of the repo checkout - never hard code Z:\\GWorkspace\\spark_esri."""
    return _REPO_ROOT


def skip(reason: str) -> "None":
    print(f"SKIP: {reason}")
    sys.exit(SKIP_EXIT_CODE)


def require(module_name: str, hint: str = ""):
    """Import an optional dependency, or SKIP the whole script when it is absent."""
    try:
        return importlib.import_module(module_name)
    except ImportError:
        skip(f"'{module_name}' is not installed. {hint}".strip())


def start(config=None):
    """Start Spark and return (spark, sql).

    Several notebooks were written for an environment where 'spark' and a bare 'sql()'
    are pre-injected globals; returning sql here is what lets those cells port verbatim.
    """
    sys.path.insert(0, os.path.join(_REPO_ROOT, "python"))
    from spark_esri import spark_start

    merged = {"spark.driver.memory": "4G", "spark.executor.memory": "4G"}
    merged.update(config or {})
    spark = spark_start(merged)
    return spark, spark.sql


def stop() -> None:
    from spark_esri import spark_stop
    spark_stop()


def check(label: str, actual, expected) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"  ok  {label} == {actual!r}")


def report(label: str, value) -> None:
    print(f"  ..  {label} = {value!r}")
