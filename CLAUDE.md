# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A Python package (`spark_esri`) that lets Apache Spark run inside a Jupyter notebook embedded in ArcGIS Pro, by launching Spark against the JVM/Java runtime bundled with Pro instead of a standalone Spark install. The rest of the repo is a collection of Jupyter notebooks that demonstrate spatial analytics on top of it (spatial binning, H3 hexbin aggregation, micro-pathing, virtual gate crossings, taxi-trip-duration ML, remote execution on Databricks, etc.), plus an ArcGIS Python Toolbox for Parquet import/export.

This is **not** a typical app repo — there is no linter or CI config. Development happens interactively inside ArcGIS Pro's bundled Jupyter environment (or Jupyter Lab for the Databricks-connect notebooks). There is a headless smoke-test suite under `tests/` (see below) covering the six notebooks that need neither a live Pro map nor external data, plus the whole `insert_cursor` package; everything else is still validated by running the notebook by hand.

## Runtime environment constraints

- Windows-only, and only functional **inside ArcGIS Pro's Python environment** — `python/spark_esri` imports `arcpy` and `winreg` at module load time, so it cannot be imported or exercised outside Pro (plain CPython on another OS fails at import).
- `spark_start()` (in `python/spark_esri/__init__.py`) locates Pro's install dir via `arcpy.GetInstallInfo()`, points `JAVA_HOME`/`HADOOP_HOME`/`SPARK_HOME` at Pro's bundled `Java/runtime` folder, resolves the active conda env's `python.exe` for `PYSPARK_PYTHON`, and manually launches/tears down the py4j gateway (stock `pyspark.java_gateway.launch_gateway`, passing `popen_kwargs` to suppress stdout/stderr — required or the JVM dies immediately inside Pro) so the JVM subprocess can be explicitly killed on `spark_stop()` via Windows `taskkill /f /t`, since Pro's embedded JVM won't die from `spark.stop()` alone.
- **`SPARK_HOME` overrides Pro's bundled Spark**, and `_bootstrap_sys_path` supports three layouts: Pro's bundled zips, a pip-installed pyspark, and a downloaded Spark distro. The discriminator is **whether `pyspark` is already importable** — *not* the presence of `python/lib/*.zip`, because a pip-installed pyspark ships those zips too. If pyspark imports, `sys.path` is deliberately left untouched rather than shadowed with a zip from a different Spark home.
- **Pro 3.7.1 bundles Spark 4.1.1, which cannot run any executor-side Python** on Windows + Python 3.12+ (upstream SPARK-53759, Esri KB 000039267; fixed in 3.5.9 / 4.0.3 / 4.1.2). `@udf`, `@pandas_udf` and `rdd.map()` all die with `Python worker exited unexpectedly (crashed)` / `WinError 10038`. Driver-side SQL, DataFrames, `toPandas()` and `toLocalIterator()` are unaffected — which is why the map-drawing notebooks still work on the bundled Spark. Remedy is `pip install "pyspark>=4.1.2"` + `SPARK_HOME`. `check_python_worker_support()` warns on the affected combination; `_needs_spark53759_fix()` is branch-aware, so don't "simplify" it to a flat `< 4.1.2`.
- **Spark 4 defaults ANSI SQL on; `spark_start` turns it back off** via `SPARK3_SQL_COMPAT` so the pre-ANSI notebooks keep working (`1/0` → null, lenient casts). User config is applied *after* it, so callers can re-enable.
- Because of the manual gateway lifecycle, always pair `spark_start()` with `spark_stop()` — leftover `PYSPARK_GATEWAY_PORT`/`PYSPARK_GATEWAY_SECRET` env vars or JVM subprocesses from a prior run will cause the next `spark_start()` to misbehave (both functions clear them). Note `spark_stop()` must not call `SparkSession.builder.getOrCreate()`: on Spark 4 that *creates* a new (possibly Spark Connect) session when none is active.
- A `@pandas_udf("double")` — DDL **string** return type — is parsed through the JVM, so it can only be declared once a session exists. `@pandas_udf(DoubleType())` takes a Python object and has no such ordering constraint. This bites when porting notebook cells to scripts, where the session isn't pre-injected.

## Package layout

- `python/spark_esri/__init__.py` — main entry point: `spark_start(config: Dict={}, probe_udf: bool=False)` / `spark_stop()`. This is what notebooks import (`from spark_esri import spark_start, spark_stop`). `probe_udf=True` actually runs one executor-side task to turn the version heuristic into a fact.
- `python/spark_dbconnect/__init__.py` — an older/parallel variant of `spark_start`/`spark_stop` for the Databricks-connect workflow. Per the README TODO, this and `spark_esri` are meant to be unified eventually but currently duplicate logic.
- `python/insert_cursor/__init__.py` — helpers to materialize a Spark `DataFrame` into an in-memory (or on-disk) ArcGIS feature class via `arcpy.da.InsertCursor`, so Spark query results can be visualized on a Pro map. Key entry points: `insert_df` (WKB/WKT shape column), `insert_df_xy` (separate x/y columns → points), `insert_df_hex` (H3-style hex index column, requires the optional [`gridhex`](https://github.com/mraad/grid-hex) package), and `insert_df_progress` (same as `insert_df` but drives Pro's progress bar and supports cancellation). All of these assume the DataFrame's leading column(s) are the geometry — trailing columns become feature class attributes via `_df_to_fields`, which maps Spark types to Esri field types (`IntegerType`/`LongType`→`LONG`, `Float`/`Double`/`Decimal`→`DOUBLE`, `Date`/`Timestamp`→`DATE`, everything else→`STRING`).
- `tools/ParquetToolbox.pyt` — an ArcGIS Python Toolbox (loaded directly into Pro, not via pip) with `ExportTool`/`ImportTool` for converting feature classes to/from Parquet on local disk or S3/GCS (via optional `boto3`/`s3fs`/`gcsfs`).
- `*.ipynb` at the repo root — worked examples; each is effectively a "how to use this feature" doc. Notably `spark_esri.ipynb` (spatial binning), `H3*.ipynb` (H3 hex aggregation variants: broadcast join, local iterator, pandas UDF), `micro_path.ipynb`, `virtual_gates.ipynb`, `taxi_trips_duration_*.ipynb` (ML pipeline), `spark_dbconnect*.ipynb` (remote Databricks execution).

## Build / install

There is no build step beyond standard `setuptools` packaging (`pyproject.toml` declares `setuptools.build_meta`; actual package metadata lives in `setup.py`/`setup.cfg`, both must stay in sync — e.g. `version`).

```commandline
# Install into the active (ArcGIS Pro) conda env, from repo root:
python setup.py install

# Build distributables (writes to dist/):
sh egg_wheel.sh        # bdist_egg + bdist_wheel
```

Packages are discovered under `python/` (`package_dir={"": "python"}`), so new top-level packages must live under `python/<name>/`.

## Environment variables that affect Spark startup

- `SPARK_HOME` — if set, overrides Pro's bundled Spark install. Read **at import time**, so changing it mid-session requires a kernel restart (`spark_start` warns if it changed). **On this machine it is already set persistently** (user scope, `HKCU\Environment`) to `%CONDA_PREFIX%\Lib\site-packages\pyspark` — i.e. the pip-installed pyspark 4.1.3, not Pro's bundled 4.1.1 — so the tests and notebooks get working Python UDFs by default. Because `setx` stored the *expanded* absolute path, it is pinned to the `spark-esri` conda env; re-run `setx` after a `proswap` to a different env.
- `PYSPARK_PYTHON` — must point at the active conda env's `python.exe`. `spark_start` auto-detects via `CONDA_PREFIX` → `CONDA_DEFAULT_ENV` (only if absolute — it usually holds just the env *name*) → `sys.prefix` → legacy `proenv.txt` → registry `HKCU\SOFTWARE\ESRI\ArcGISPro\PythonCondaEnv` → `arcgispro-py3` with a warning. An already-set value is respected.
- `HADOOP_HOME` — auto-set to Pro's bundled `Java/runtime/hadoop` unless already present in the environment.
- `PYSPARK_GATEWAY_PORT` / `PYSPARK_GATEWAY_SECRET` — must NOT be left over from a previous session; `spark_start` deletes them defensively.
- `SPARK_ESRI_NO_WARN=1` silences the SPARK-53759 warning; `SPARK_ESRI_STRICT=1` raises instead of warning (for CI).
- `SPARK_ESRI_TEST_ROWS` — row count for the `tests/` suite (default 50000).

## Tests

```commandline
python tests\run_all.py            # or: python tests\run_all.py t_qr
```

`SPARK_HOME` is already set persistently on this machine (see above), so no per-invocation setup is needed. `run_all.py` echoes the resolved `SPARK_HOME` on its first line — if that shows Pro's bundled Spark, every UDF test will fail with SPARK-53759.

Plain scripts, no Jupyter. `run_all.py` runs each `t_*.py` in its own subprocess (a Spark session is a process-global singleton that can't be cleanly rebuilt in-process) and reports PASS / SKIP / FAIL, exiting non-zero only on FAIL. Exit code `77` means SKIP — used when an optional dep is missing. `numba`, `h3` and `gridhex` are all installed on this machine, so a clean run is **8 passed / 0 skipped / 0 failed**.

`gridhex` is GitHub-only (not on PyPI) and is wired up from a local clone via a `.pth` file in site-packages (`gridhex-dev.pth` → `<clone>/src/main/python`) rather than `pip install -e`. On a mapped/shared drive pip rewrites the drive letter to UNC and dies with `WinError 3`, and legacy `setup.py develop` leaves an `egg-link` plus an **empty** `easy-install.pth`, so the package stays unimportable while pip reports success. Python imports from such drives fine — only pip's normalization breaks. See the README for the command.

`_harness` puts `python/` on `sys.path` **at import time**, not inside `start()`, so every test can `import insert_cursor` / `import spark_esri` at module scope and still run standalone (`python tests\t_insert_cursor.py`), not only via `run_all.py`.

**`arcpy` does not need an open ArcGIS Pro project.** `t_insert_cursor.py` exercises the entire `insert_cursor` package headlessly — `CreateFeatureclass`/`InsertCursor`/`SearchCursor` against the `memory` workspace, and the progressor APIs, all work in standalone arcpy at ArcInfo level. Only `arcpy.mp` and named map layers require a live project. Always open `SearchCursor` under `with`: an unreleased cursor holds a schema lock and makes a later `Delete` fail intermittently.

`t_spark_version.py` is the one that matters after a Pro or Spark upgrade: it exercises `rdd.map()`, `@udf` and `@pandas_udf`, so it fails loudly if SPARK-53759 regresses, and it unit-tests `_needs_spark53759_fix` against the full affected/fixed version matrix.

## Working with notebooks

The `tests/` suite covers the six notebooks needing neither a live map nor external data, plus `insert_cursor` directly. For everything else, verifying a change still means running the notebook end-to-end inside ArcGIS Pro and confirming features render on the map — 13 notebooks need a live layer (`Broadcast`, `Gates`, `Slicks39N`, `Predictions`), and 8 need a remote cluster, the proprietary `sparkgeo`/`esri_spark` jars, MinIO, a GPU, or IPython magics. Note the blocker there is reading a *named map layer*, not `insert_cursor` itself — writing the results back out is covered by `t_insert_cursor.py`.

Several notebooks carry pre-existing latent bugs that are *not* Spark-4 related and remain unfixed: missing `import math` / `import arcpy`, an undefined bare `sql()` in the cells written for a Zeppelin-style pre-injected environment, and `SparkJoinOnGPU.ipynb` joining a view `v2` that is never created. `H3.ipynb` / `SparkGeo*.ipynb` / `sparkgeo_in_pro.ipynb` also pin `sparkgeo` jars built for Spark 3.0–3.5, which are binary-incompatible with Spark 4.
