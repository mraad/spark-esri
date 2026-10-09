#
# Code borrowed and modified from https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
#
import glob
import importlib
import os
import re
import subprocess
import sys
import winreg
from importlib.util import find_spec
from typing import Dict, Optional, Tuple

import arcpy

__version__ = "0.13"

pro_home = arcpy.GetInstallInfo()["InstallDir"]
pro_runtime_dir = os.path.join(pro_home, "Java", "runtime")
pro_spark_home = os.path.join(pro_runtime_dir, "spark")


def _clean_path(path: str) -> str:
    """Normalize a user supplied path - strip quotes, whitespace and trailing separators."""
    return os.path.normpath(os.path.expandvars(os.path.expanduser(path.strip().strip('"').strip("'"))))


def _is_spark_home(path: str) -> bool:
    """True when the folder looks like a Spark installation.

    Holds for all 3 supported layouts:
      - the Spark that ships with Pro   (...\\Java\\runtime\\spark)
      - a downloaded Spark distribution (spark-4.1.3-bin-hadoop3)
      - a pip installed pyspark         (<site-packages>\\pyspark)
    """
    if not path or not os.path.isdir(path):
        return False
    return os.path.isfile(os.path.join(path, "bin", "spark-submit.cmd")) and \
        os.path.isdir(os.path.join(path, "jars"))


def _auto_spark_home() -> str:
    """Spark home to use when SPARK_HOME is not usable.

    A pip installed pyspark that is importable from the active env wins over the Spark that
    ships with Pro: its python code is what gets imported regardless, so pairing it with Pro's
    jars is a guaranteed version mismatch. A pip pyspark carries its own jars and launcher.
    """
    origin = _module_origin("pyspark")
    if origin is not None:
        home = os.path.dirname(origin)
        if _is_spark_home(home):
            return home
    return pro_spark_home


def _resolve_spark_home() -> str:
    """SPARK_HOME wins over the Spark that ships with Pro."""
    if "SPARK_HOME" not in os.environ:
        return _auto_spark_home()

    given = os.environ["SPARK_HOME"]
    home = _clean_path(given)
    if not os.path.exists(home):
        # Typically a 'setx SPARK_HOME %CONDA_PREFIX%\...' that pinned the expanded path of a
        # conda env which has since been deleted or recreated under another name.
        fallback = _auto_spark_home()
        print(f"***WARNING*** SPARK_HOME='{given}' does not exist (a deleted or renamed conda env?). "
              f"Using '{fallback}' instead - update or unset SPARK_HOME to silence this.")
        return fallback
    if not _is_spark_home(home):
        # Be forgiving - a pip install is rooted at <site-packages>\pyspark, users
        # tend to point SPARK_HOME at the env root or at site-packages instead.
        for candidate in (os.path.join(home, "pyspark"),
                          os.path.join(home, "Lib", "site-packages", "pyspark"),
                          os.path.join(home, "site-packages", "pyspark")):
            if _is_spark_home(candidate):
                home = candidate
                break
    if not _is_spark_home(home):
        raise RuntimeError(
            f"SPARK_HOME='{given}' is not a Spark installation - expecting "
            f"'bin\\spark-submit.cmd' and a 'jars' folder below it.\n"
            f"Unset SPARK_HOME to fall back on a pip installed pyspark in the active env, "
            f"else on the Spark that ships with ArcGIS Pro ('{pro_spark_home}').")
    return home


def _spark_home_version(home: str) -> Optional[str]:
    """Version of the jars in a Spark home, read from spark-core_<scala>-<version>.jar."""
    for jar in glob.glob(os.path.join(home, "jars", "spark-core_*.jar")):
        return os.path.basename(jar)[:-len(".jar")].rsplit("-", 1)[-1]
    return None


def _module_origin(name: str) -> Optional[str]:
    try:
        spec = find_spec(name)
    except (ImportError, ValueError):
        return None
    return None if spec is None or spec.origin is None else spec.origin


def _bootstrap_sys_path(home: str) -> None:
    """Make 'pyspark' and 'py4j' importable for the given Spark home.

    If pyspark is already importable (pip installed pyspark, databricks-connect, ...)
    sys.path is deliberately left alone - prepending a zip from a *different* Spark
    home is exactly how you get a python/JVM version mismatch that fails much later.
    Note a pip installed pyspark does ship 'python/lib/py4j-*-src.zip', so probing for
    that zip is NOT a reliable way to tell the layouts apart - importability is.
    """
    already = _module_origin("pyspark")
    if already is not None:
        installed_home = os.path.dirname(already)
        if os.path.normcase(installed_home) != os.path.normcase(home):
            print(f"***WARNING*** 'pyspark' is already importable from '{installed_home}' "
                  f"but SPARK_HOME is '{home}'. The importable copy wins; set SPARK_HOME "
                  f"to '{installed_home}' or uninstall it to avoid a version mismatch.")
        return

    py_dir = os.path.join(home, "python")
    pyspark_zip = os.path.join(py_dir, "lib", "pyspark.zip")
    if os.path.isfile(pyspark_zip):
        # Pro's Spark and any downloaded distribution. Preferred over <home>\python because
        # that folder also exposes lib/, docs/ and test_support/ as namespace packages.
        sys.path.insert(0, pyspark_zip)
    elif os.path.isfile(os.path.join(py_dir, "pyspark", "__init__.py")):
        sys.path.insert(0, py_dir)
    elif os.path.basename(home).lower() == "pyspark" and \
            os.path.isfile(os.path.join(home, "__init__.py")):
        # A pip layout that is simply not on sys.path (e.g. another conda env).
        sys.path.insert(0, os.path.dirname(home))
    else:
        raise RuntimeError(
            f"Cannot locate the pyspark python code under '{home}' - looked for "
            f"'python\\lib\\pyspark.zip' and 'python\\pyspark'.")

    if _module_origin("py4j") is None:
        py4j_zips = sorted(glob.glob(os.path.join(py_dir, "lib", "py4j-*-src.zip")))
        if not py4j_zips:
            raise RuntimeError(
                f"Cannot locate py4j under '{os.path.join(py_dir, 'lib')}'. "
                f"Run 'pip install py4j' or point SPARK_HOME at a full Spark distribution.")
        sys.path.insert(0, py4j_zips[-1])

    importlib.invalidate_caches()


_spark_home_env = os.environ.get("SPARK_HOME")  # as seen at import time
spark_home = _resolve_spark_home()
_bootstrap_sys_path(spark_home)

import pyspark  # noqa: E402
from pyspark import SparkContext, SparkConf  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.java_gateway import launch_gateway  # noqa: E402

SparkContext._gateway = None

# Spark 4 turns ANSI SQL on by default; the notebooks in this repo were written against
# Spark 3 semantics (1/0 -> null rather than DIVIDE_BY_ZERO, lenient CAST). Override with
# spark_start({"spark.sql.ansi.enabled": True}) - or drop this once the notebooks are audited.
SPARK3_SQL_COMPAT = {
    "spark.sql.ansi.enabled": "false",
}

_warned_53759 = False


def _version_tuple(text: str) -> Tuple[int, int, int]:
    parts = []
    for token in re.split(r"[.\-+_]", text or ""):
        if token.isdigit():
            parts.append(int(token))
        else:
            break
    parts += [0] * (3 - len(parts))
    return tuple(parts[:3])


def _needs_spark53759_fix(version_text: str) -> bool:
    """SPARK-53759 'Fix missing flush in simple-worker path' - fixed in 3.5.9 / 4.0.3 / 4.1.2."""
    major, minor, patch = _version_tuple(version_text)
    if (major, minor) == (4, 1):
        return patch < 2
    if (major, minor) == (4, 0):
        return patch < 3
    if (major, minor) == (3, 5):
        return patch < 9
    return (major, minor) < (3, 5)  # 4.2+ and later branches carry the fix


def check_python_worker_support(verbose: bool = True) -> bool:
    """Warn when the active Spark cannot run executor side python (SPARK-53759).

    Returns True when python workers are expected to work.
    Symptom when it does not: 'Python worker exited unexpectedly (crashed)' followed by
    'WinError 10038' from rdd.map(), @udf and @pandas_udf. Driver side SQL, DataFrame,
    toPandas() and toLocalIterator() are NOT affected.
    """
    global _warned_53759
    version = getattr(pyspark, "__version__", "0")
    broken = (sys.platform == "win32"
              and sys.version_info >= (3, 12)
              and _needs_spark53759_fix(version))
    if not broken:
        return True
    if os.environ.get("SPARK_ESRI_STRICT", "").lower() in ("1", "true", "yes"):
        raise RuntimeError(
            f"pyspark {version} cannot run python workers on Windows/Python "
            f"{sys.version_info.major}.{sys.version_info.minor} (SPARK-53759). "
            f"Set SPARK_HOME to a Spark >= 4.1.2 - see Esri KB 000039267.")
    if verbose and not _warned_53759:
        _warned_53759 = True
        print(f"""
***WARNING*** pyspark {version} is affected by SPARK-53759 (missing flush in the
              simple-worker path). On Windows with Python {sys.version_info.major}.{sys.version_info.minor},
              rdd.map(), @udf and @pandas_udf will fail with
                  'Python worker exited unexpectedly (crashed)'  /  [WinError 10038]
              Spark SQL, DataFrame, toPandas() and toLocalIterator() are NOT affected.
              This is an upstream Spark bug, not an ArcGIS Pro or spark-esri bug - see
              Esri KB 000039267. There is no configuration workaround: Spark always uses
              the simple-worker path on Windows.
              Fixed in Spark 3.5.9 / 4.0.3 / 4.1.2. To pick up the fix:
                  pip install pyspark==4.1.3
                  set SPARK_HOME=%CONDA_PREFIX%\\Lib\\site-packages\\pyspark
              then RESTART the notebook kernel (SPARK_HOME is read at import time).
              Set SPARK_ESRI_NO_WARN=1 to silence, SPARK_ESRI_STRICT=1 to raise instead.
""".rstrip())
    return False


def _probe_python_worker(spark) -> bool:
    """Actually run one executor side python task. Costs a few seconds, so opt-in."""
    try:
        spark.range(1).rdd.map(lambda row: row[0]).collect()
        return True
    except Exception as ex:  # noqa: BLE001
        print(f"***WARNING*** Executor side python is NOT working: {type(ex).__name__}: {ex}")
        check_python_worker_support()
        return False


def _set_pyspark_python() -> None:
    if "PYSPARK_PYTHON" in os.environ:
        return  # explicit user setting always wins

    def _accept(folder: Optional[str]) -> bool:
        if not folder or not os.path.isabs(folder):
            return False
        python_exe = os.path.join(folder, "python.exe")
        if os.path.exists(python_exe):
            os.environ["PYSPARK_PYTHON"] = python_exe
            return True
        return False

    # CONDA_PREFIX is the path; CONDA_DEFAULT_ENV is only the *name* ("spark-esri") unless
    # the env lives outside the envs folder - hence the isabs() guard inside _accept().
    if _accept(os.getenv("CONDA_PREFIX")):
        return
    if _accept(os.getenv("CONDA_DEFAULT_ENV")):
        return
    # Works even in Pro's embedded interpreter, where sys.executable is ArcGISPro.exe.
    if _accept(sys.prefix):
        return

    if "LOCALAPPDATA" in os.environ:
        # Pre Pro 2.8
        pro_env_txt = os.path.join(os.getenv("LOCALAPPDATA"), "ESRI", "conda", "envs", "proenv.txt")
        if os.path.exists(pro_env_txt):
            with open(pro_env_txt, "r") as fp:
                if _accept(fp.read().strip()):
                    return

    try:
        # Pro 2.8
        with winreg.ConnectRegistry(None, winreg.HKEY_CURRENT_USER) as key_node:
            sub_node = os.path.join("SOFTWARE", "ESRI", "ArcGISPro")
            with winreg.OpenKey(key_node, sub_node) as sub_key:
                conda_env, _ = winreg.QueryValueEx(sub_key, "PythonCondaEnv")
                if _accept(conda_env):
                    return
    except OSError:  # WindowsError is a deprecated alias of OSError
        pass

    os.environ["PYSPARK_PYTHON"] = os.path.join(pro_home, "bin", "Python", "envs", "arcgispro-py3", "python.exe")
    print("***WARNING*** Falling back on arcgispro-py3 python.")


def spark_start(config: Dict = {}, probe_udf: bool = False) -> SparkSession:
    if SparkContext._gateway is not None:
        return SparkSession.builder.getOrCreate()

    # Compare against the raw value seen at import - spark_start() below overwrites SPARK_HOME
    # with the resolved home, so a second spark_start() in the same kernel compares equal too.
    def _norm(path: Optional[str]) -> str:
        return os.path.normcase(_clean_path(path)) if path else ""

    current = os.environ.get("SPARK_HOME")
    if _norm(current) not in (_norm(_spark_home_env), _norm(spark_home)):
        print(f"***WARNING*** SPARK_HOME changed to '{current}' after spark_esri was imported. "
              f"Still using '{spark_home}' - restart the notebook kernel to pick up the change.")

    jar_version = _spark_home_version(spark_home)
    if jar_version and jar_version != getattr(pyspark, "__version__", jar_version):
        print(f"***WARNING*** pyspark {pyspark.__version__} against Spark {jar_version} jars "
              f"in '{spark_home}' - mismatched versions are not supported.")

    os.environ["JAVA_HOME"] = os.path.join(pro_runtime_dir, "jre")
    if "HADOOP_HOME" not in os.environ:
        hadoop_home = os.path.join(pro_runtime_dir, "hadoop")
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")
    os.environ["SPARK_HOME"] = spark_home
    # Stock launch_gateway defaults this to "true" (ClientServer). Pin it explicitly so the
    # behaviour is visible; set PYSPARK_PIN_THREAD=false to fall back on a plain JavaGateway,
    # which is what the removed vendored gateway used to do.
    os.environ.setdefault("PYSPARK_PIN_THREAD", "true")
    # spark-submit would otherwise be free to start in Spark Connect mode.
    os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    if "SPARK_REMOTE" in os.environ:
        print(f"***WARNING*** SPARK_REMOTE='{os.environ['SPARK_REMOTE']}' is set; "
              f"spark_esri starts a local classic session and ignores it.")
    # Set python.exe based on the active conda env.
    _set_pyspark_python()
    #
    # these need to be reset on every run or pyspark will think the Java gateway is still up and running
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        del os.environ["PYSPARK_GATEWAY_PORT"]
    if "PYSPARK_GATEWAY_SECRET" in os.environ:
        del os.environ["PYSPARK_GATEWAY_SECRET"]
    SparkContext._jvm = None
    SparkContext._gateway = None

    conf = SparkConf()
    conf.set("spark.master", "local[*]")
    conf.set("spark.driver.host", "127.0.0.1")  # Added per suggestion from ctoledo-img-com-br :-)
    conf.set("spark.driver.memory", "32G")
    conf.set("spark.executor.memory", "32G")
    conf.set("spark.ui.enabled", False)
    conf.set("spark.ui.showConsoleProgress", False)
    conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
    conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", True)
    conf.set("spark.sql.catalogImplementation", "in-memory")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    for k, v in SPARK3_SQL_COMPAT.items():
        conf.set(k, v)
    # Add/Update user defined spark configurations - these must come last so the user wins.
    for k, v in config.items():
        conf.set(k, v)

    # we have to manage the py4j gateway ourselves so that we can control the JVM process
    popen_kwargs = {
        'stdout': subprocess.DEVNULL,  # need to redirect stdout & stderr when running in Pro or JVM fails immediately
        'stderr': subprocess.DEVNULL,
        'shell': True  # keeps the command-line window from showing
    }
    gateway = launch_gateway(conf=conf, popen_kwargs=popen_kwargs)
    sc = SparkContext(gateway=gateway)
    spark = SparkSession(sc)
    # Kick-start the spark engine.
    spark.sql("select 1").collect()
    if os.environ.get("SPARK_ESRI_NO_WARN", "").lower() not in ("1", "true", "yes"):
        check_python_worker_support()
    if probe_udf:
        _probe_python_worker(spark)
    return spark


def spark_stop() -> None:
    gateway = SparkContext._gateway
    if gateway is None:
        print("***WARNING*** No active Spark session.")
        return
    try:
        # Do NOT use SparkSession.builder.getOrCreate() here - on Spark 4 it will happily
        # create a *new* (possibly Spark Connect) session when none is active.
        session = SparkSession._instantiatedSession
        if session is not None:
            session.stop()
        elif SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
    finally:
        proc = getattr(gateway, "proc", None)
        try:
            gateway.shutdown()
        except Exception:  # noqa: BLE001 - the JVM may already be gone
            pass
        if proc is not None:
            if proc.stdin is not None:
                proc.stdin.close()
            # ensure that process and all children are killed
            subprocess.Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(proc.pid)],
                             shell=True,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        SparkContext._gateway = None
        SparkContext._jvm = None
        SparkContext._active_spark_context = None
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
        os.environ.pop("PYSPARK_GATEWAY_PORT", None)
        os.environ.pop("PYSPARK_GATEWAY_SECRET", None)
