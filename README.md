# Spark Esri

Project to demonstrate the usage of [Apache Spark](https://spark.apache.org/) within a [Jupyter notebook within ArcGIS Pro](https://pro.arcgis.com/en/pro-app/arcpy/get-started/pro-notebooks.htm).

## Notes

Aug 2, 2026 - Updated for Pro 3.7, which bundles **Spark 4.1.1** on Python 3.13. Three things changed:

- **Python UDFs are broken on Pro's bundled Spark.** See [Known issue: python UDFs on Windows](#known-issue-python-udfs-on-windows-spark-53759) below - you need `SPARK_HOME` pointing at Spark >= 4.1.2.
- **ANSI SQL is turned back off.** Spark 4 enables `spark.sql.ansi.enabled` by default, which makes `1/0` raise `DIVIDE_BY_ZERO` instead of returning null. `spark_start` sets it back to `false` so the notebooks in this repo keep their Spark 3 semantics. Opt back in with `spark_start({"spark.sql.ansi.enabled": True})`.
- **The vendored `spark.java_gateway` module is gone.** Stock `pyspark.java_gateway.launch_gateway` supports the `popen_kwargs` argument this project needs, so the copy was deleted. If you imported `from spark.java_gateway import launch_gateway`, import it from `pyspark.java_gateway` instead.

Oct 25, 2022 - Updated to support upcoming Pro 3.1. See SparkGeo2 notebook for integration with Apache Arrow :-)

Apr 12, 2022 - Running PySpark in Pro 2.9 requires the `PYSPARK_PYTHON` environment variable to be set. It should point to the python.exe executable of your active conda environment, e.g., `C:\Users\%USERNAME%\AppData\Local\ESRI\conda\envs\spark_esri\python.exe`. Defining `CONDA_DEFAULT_ENV` is neither sufficient and nor necesary.

Dec 16, 2021 - Added check for env var `SPARK_HOME` to override built-in spark. See instructions below.

Oct 30, 2021 - Pro 2.8 relies on the Windows registry to find the active conda environment. The registry key is `HKEY_CURRENT_USER/SOFTWARE/ESRI/ArcGISPro/PythonCondaEnv`. The value of this key is used to set the required os environment variable `PYSPARK_PYTHON` for PySpark to work correctly in a Pro notebook.

As of this writing, the order to detect the active conda environment is as follows:

- if `PYSPARK_PYTHON` is already set, it is left alone.
- look for env var `CONDA_PREFIX` (the env *path*).
- look for env var `CONDA_DEFAULT_ENV`, but only when it holds an absolute path - on a
  normal `proswap` it holds just the env *name* (e.g. `spark-esri`), which is why this
  check alone was never enough.
- look for `sys.prefix`, which is correct even inside Pro's embedded interpreter where
  `sys.executable` is `ArcGISPro.exe`.
- look for `%LOCALAPPDATA%/ESRI/conda/envs/proenv.txt`, in case of an older Pro version.
- look for `HKEY_CURRENT_USER/SOFTWARE/ESRI/ArcGISPro/PythonCondaEnv`.

Oct 27, 2021 - Pro 2.8.3 removed the reliance and existence of the file `%LOCALAPPDATA%/ESRI/conda/envs/proenv.txt`. It now depend on env var `CONDA_DEFAULT_ENV` to determine the activate conda env.

~~Sep 16, 2021 - Perform the following as a patch for Pro 2.8.3~~

```commandline
cd c:\
git clone https://github.com/kontext-tech/winutils
```

~~Define a system environment variable `HADOOP_HOME` with value `C:\winutils\hadoop-3.3.0` and add to system variable `PATH` the `%HADOOP_HOME%/bin` value.~~

~~NOTE: This works in Pro 2.6 ONLY. There is a small "issue" with Pro 2.7 and pyarrow. The folks in Redlands have a fix that will be in 2.8 :-(~~

## Known issue: python UDFs on Windows (SPARK-53759)

On Windows with Python 3.12+, Spark versions before **3.5.9 / 4.0.3 / 4.1.2** cannot run
executor-side python at all. `rdd.map()`, `@udf` and `@pandas_udf` all die with:

```text
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
OSError: [WinError 10038] An operation was attempted on something that is not a socket
```

This is upstream bug [SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759)
("Fix missing flush in the simple-worker path"), documented by Esri as
[KB 000039267](https://support.esri.com/en-us/knowledge-base/pyspark-crashes-with-python-3-12-on-windows-000039267).
It is **not** a bug in ArcGIS Pro or in this project, and it reproduces with a stock
`arcgispro-py3` environment. There is no configuration workaround - Spark always takes the
simple-worker path on Windows, so `spark.python.use.daemon` is already effectively `false`.

Driver-side work is unaffected: Spark SQL, DataFrames, `toPandas()` and `toLocalIterator()`
all work fine, which is why the binning / micropath / gate-crossing notebooks still run on
Pro's bundled Spark.

**ArcGIS Pro 3.7 bundles Spark 4.1.1**, which is affected. To pick up the fix:

```commandline
pip install "pyspark>=4.1.2"
setx SPARK_HOME "%CONDA_PREFIX%\Lib\site-packages\pyspark"
```

`setx` writes the variable permanently (current user, `HKCU\Environment`) so every new
session gets it. Use `set` instead of `setx` if you only want it for the current prompt.
Either way it takes effect for **newly launched** processes only - restart ArcGIS Pro and
any open terminals, and restart the notebook kernel, since `SPARK_HOME` is read when
`spark_esri` is imported.

Note `setx` expands `%CONDA_PREFIX%` at the time you run it, storing the resulting absolute
path. That pins `SPARK_HOME` to the conda env that was active - if you later `proswap` to a
different env, re-run the `setx` from that env. `spark_start()` warns when the pyspark it
can import disagrees with `SPARK_HOME`, rather than failing silently.

`spark_start()` also prints a warning when it detects the affected Spark/Python combination;
set `SPARK_ESRI_NO_WARN=1` to silence it or `SPARK_ESRI_STRICT=1` to raise instead.

## Installation

### Install Spark (Optional).

If you do not wish to use Pro's built-in Spark, you can override it by setting the
environment variable `SPARK_HOME`. Three layouts are supported:

- Pro's bundled Spark (the default when `SPARK_HOME` is unset).
- A pip-installed pyspark - `pip install "pyspark>=4.1.2"`, then
  `setx SPARK_HOME "%CONDA_PREFIX%\Lib\site-packages\pyspark"`. This is the easiest way to
  get the SPARK-53759 fix and is the recommended setup on Pro 3.7.
- A downloaded Spark distribution - extract e.g. `spark-4.1.3-bin-hadoop3.tgz` and point
  `SPARK_HOME` at the folder. It's best to avoid spaces in the folder path.

If `pyspark` is already importable, `spark_esri` leaves `sys.path` alone rather than
shadowing it with a zip from a different Spark home, and warns when the importable copy
disagrees with `SPARK_HOME`.


### Create a new Pro Conda Environment.

Start a `Python Command Prompt`:

![](media/Command.png)

**Note**: You _might_ need to add proxy settings to `.condarc` located in `C:\Program Files\ArcGIS\Pro\bin\Python`.

```commandline
conda config --set proxy_servers.http http://username:password@host:port
conda config --set proxy_servers.https https://username:password@host:port
```

The above will produce something like the below:

```text
ssl_verify: true
proxy_servers:
  http: http://domainname\username:password@host:port
  https: http://domainname\username:password@host:port
```

Create a new conda environment:

```commandline
proswap arcgispro-py3
conda remove --yes --all --name spark_esri
conda create --yes --name spark_esri --clone arcgispro-py3
proswap spark_esri
```

Optional extras, by what needs them:

```commandline
pip install numba          ; QR, SparkPandas, MercatorUDF notebooks + their tests
pip install h3             ; H3_* notebooks + t_h3_pandas_udf
pip install boto3 s3fs     ; ParquetToolbox S3 export/import, minio notebook
pip install gcsfs          ; ParquetToolbox GCS export/import
```

**Do not pin the old versions this README used to recommend** (`pandas==1.2`,
`pyarrow==1.0.1`, `numba==0.53`, `s3fs==0.4.2`). Spark 4.1.x requires `pandas>=2.2.0`,
`pyarrow>=15.0.0` and `numpy>=1.22`; the old pins will break it. Let pip resolve current
versions against the ones Pro already ships.

Last verified on ArcGIS Pro 3.7.1 / Python 3.13.13 with pyspark 4.1.3, pandas 3.0.0,
numpy 2.3.5, pyarrow 22.0.0, numba 0.66.0 and h3 4.5.0 - all six smoke tests passing.

Install the Esri Spark module.

**Note**: You _might_ need to install [Git for Windows](https://gitforwindows.org).

```commandline
git clone https://github.com/mraad/spark-esri.git
cd spark-esri
pip install .
```

## Tests

Headless smoke tests live in [tests/](tests). They are plain scripts - no Jupyter and no
open ArcGIS Pro project required - and each runs in its own subprocess because a Spark
session is a process-global singleton.

```commandline
python tests\run_all.py                  ; everything
python tests\run_all.py t_qr             ; a subset, by substring
```

The suite needs a Spark that can run executor-side python, i.e. `SPARK_HOME` pointing at
Spark >= 4.1.2 - see [the SPARK-53759 section](#known-issue-python-udfs-on-windows-spark-53759).
`run_all.py` echoes the `SPARK_HOME` it resolved, so a run against the wrong Spark is
obvious from the first line of output.

`t_spark_version.py` is the one to run after any Pro or Spark upgrade: it exercises
`rdd.map()`, `@udf` and `@pandas_udf`, so it fails loudly if SPARK-53759 is back, and it
unit-tests the affected/fixed version matrix.

Tests whose optional dependency is missing report `SKIP` rather than failing
(`t_qr`, `t_spark_pandas`, `t_mercator` need `numba`; `t_h3_pandas_udf` needs `h3`;
`t_insert_df_hex` needs [`gridhex`](https://github.com/mraad/grid-hex), which is not on
PyPI). Row counts default to 50k; set `SPARK_ESRI_TEST_ROWS` to run at notebook scale.

### Enabling `t_insert_df_hex` from a local grid-hex checkout

`gridhex` has to come from a clone. Point the env at it with a `.pth` file, which is what
an editable install writes anyway:

```commandline
echo <path-to>\grid-hex\src\main\python > %CONDA_PREFIX%\Lib\site-packages\gridhex-dev.pth
```

Prefer this over `pip install -e` when the clone lives on a mapped/shared drive: pip
rewrites a drive letter to its UNC form (`\\host\share\...`) and can fail with
`WinError 3`, and the legacy `setup.py develop` path can leave a `gridhex.egg-link`
behind while writing an **empty** `easy-install.pth`, so the package silently stays
unimportable. Python itself imports from such drives fine — it is only pip's path
normalization that breaks. Delete the `.pth` to undo.

`t_insert_cursor.py` covers the whole `insert_cursor` package — `insert_df`, `insert_df_xy`,
`insert_df_progress`, the low-level cursor helpers and the Spark-to-Esri field type mapping.
It needs **no open ArcGIS Pro project**: it writes to the `memory` workspace, which works in
standalone arcpy. Only `arcpy.mp` and named map layers require a live project.

The remaining notebooks are not covered because they read a live Pro map layer
(`Broadcast`, `Gates`, `Slicks39N`, `Predictions`), or need a GeoAnalytics licence, a remote
Databricks cluster, the proprietary `sparkgeo`/`esri_spark` jars, MinIO, or a GPU.

### [Spatial Binning](spark_esri.ipynb) Notebook

![](media/Notebook.png)

![](media/Pro1.png)

### [MicroPathing](micro_path.ipynb) Notebook

![](media/Micropath1.png)

Please note the usage of the [range slider](https://pro.arcgis.com/en/pro-app/help/mapping/range/get-started-with-the-range-slider.htm) on the map to filter the micropaths between a user defined hour of day.

![](media/Micropath2.png)

### [Virtual Gate Crossings](virtual_gates.ipynb) Notebook

![](media/Gates1.png)

The following is the resulting crossing points and gates statistics.

![](media/Gates2.png)

### [Remote Execution on MS Azure Databricks](spark_dbconnect.ipynb) Notebook

![](media/Cluster.png)

### [Predict Taxi Trip Durations](taxi_trips_duration_train.ipynb), [Map Taxi Trip Duration Errors](taxi_trips_duration_error.ipynb) Notebooks

![](media/TripErrors.png)

## TODO

- Unify spark_esri and spark_dbconnect python modules.

## References

- https://github.com/kontext-tech/winutils
- https://github.com/cdarlint/winutils
- https://github.com/steveloughran/winutils
- https://www.geeksforgeeks.org/check-if-two-given-line-segments-intersect/
- https://www.kite.com/python/answers/how-to-check-if-two-line-segments-intersect-in-python
- https://pandas.pydata.org/pandas-docs/stable/development/extending.html
- https://pandas.pydata.org/pandas-docs/stable/user_guide/style.html
- https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
- https://marinecadastre.gov/ais/
- https://www.movable-type.co.uk/scripts/latlong.html
- https://www.kaggle.com/c/nyc-taxi-trip-duration/data
- https://developers.google.com/maps/documentation/utilities/polylinealgorithm
- https://nvidia.github.io/spark-rapids
- https://github.com/nvidia/spark-rapids
- https://github.com/quantopian/qgrid
- https://gist.github.com/rkaneko/dd2fae35149a29405d5e287ccd62677f Put parquet file on MinIO (S3 compatible storage) using pyarrow and s3fs
- https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1
