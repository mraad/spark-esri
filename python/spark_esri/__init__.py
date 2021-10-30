#
# Code borrowed and modified from https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
#
import os
import subprocess
import sys
import winreg
from typing import Dict

import arcpy
import glob

pro_home = arcpy.GetInstallInfo()["InstallDir"]
pro_lib_dir = os.path.join(pro_home, "Java", "lib")
pro_runtime_dir = os.path.join(pro_home, "Java", "runtime")
spark_home = os.path.join(pro_runtime_dir, "spark")

# add spark/py4j libraries from Pro runtime to path for import
sys.path.insert(0, os.path.join(spark_home, "python", "lib", "pyspark.zip"))
py4j_zip = glob.glob(os.path.join(spark_home, "python", "lib", "py4j-*-src.zip"))[0]
sys.path.insert(0, py4j_zip)

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from spark.java_gateway import launch_gateway

SparkContext._gateway = None


def _set_pyspark_python() -> None:
    if "CONDA_DEFAULT_ENV" in os.environ:
        os.environ["PYSPARK_PYTHON"] = os.path.join(os.getenv("CONDA_DEFAULT_ENV"), "python.exe")

    if "PYSPARK_PYTHON" not in os.environ and "LOCALAPPDATA" in os.environ:
        # Pre Pro 2.8
        pro_env_txt = os.path.join(os.getenv("LOCALAPPDATA"), "ESRI", "conda", "envs", "proenv.txt")
        if os.path.exists(pro_env_txt):
            with open(pro_env_txt, "r") as fp:
                python_path = fp.read().strip()
                os.environ["PYSPARK_PYTHON"] = os.path.join(python_path, "python.exe")

    if "PYSPARK_PYTHON" not in os.environ:
        try:
            # Pro 2.8
            with winreg.ConnectRegistry(None, winreg.HKEY_CURRENT_USER) as key_node:
                sub_node = os.path.join("SOFTWARE", "ESRI", "ArcGISPro")
                with winreg.OpenKey(key_node, sub_node) as sub_key:
                    conda_env, _ = winreg.QueryValueEx(sub_key, "PythonCondaEnv")
                    os.environ["PYSPARK_PYTHON"] = os.path.join(conda_env, "python.exe")
        except WindowsError:
            pass

    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = os.path.join(pro_home, "bin", "Python", "envs", "arcgispro-py3", "python.exe")
        print(f"***WARNING*** Falling back on arcgispro-py3 python.")


def spark_start(config: Dict = {}) -> SparkSession:
    os.environ["JAVA_HOME"] = os.path.join(pro_runtime_dir, "jre")
    if "HADOOP_HOME" not in os.environ:
        hadoop_home = os.path.join(pro_runtime_dir, "hadoop")
        os.environ["HADOOP_HOME"] = hadoop_home
        sys.path.append(os.path.join(hadoop_home, "bin"))
    os.environ["SPARK_HOME"] = spark_home
    # Set python.exe based on the active conda env.
    _set_pyspark_python()
    #
    # these need to be reset on every run or pyspark will think the Java gateway is still up and running
    os.environ.unsetenv("PYSPARK_GATEWAY_PORT")
    os.environ.unsetenv("PYSPARK_GATEWAY_SECRET")
    SparkContext._jvm = None
    SparkContext._gateway = None

    spark_jars = [
        # os.path.join(pro_lib_dir, "spark-desktop-engine.jar"),
        # os.path.join(pro_lib_dir, "arcobjects.jar")
    ]
    spark_jars = ",".join(spark_jars)

    conf = SparkConf()
    conf.set("spark.master", "local[*]")
    conf.set("spark.driver.host", "127.0.0.1")  # Added per suggestion from ctoledo-img-com-br :-)
    conf.set("spark.ui.enabled", False)
    conf.set("spark.ui.showConsoleProgress", False)
    conf.set("spark.sql.execution.arrow.enabled", True)
    conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
    conf.set("spark.sql.catalogImplementation", "in-memory")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.jars", spark_jars)
    # Add/Update user defined spark configurations.
    for k, v in config.items():
        if k == "spark.jars":
            v = spark_jars + "," + v
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
    # Kick start the spark engine.
    spark.sql("select 1").collect()
    return spark


def spark_stop():
    if SparkContext._gateway:
        spark = SparkSession.builder.getOrCreate()
        gateway = spark._sc._gateway
        spark.stop()
        gateway.shutdown()
        gateway.proc.stdin.close()

        # ensure that process and all children are killed
        subprocess.Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(gateway.proc.pid)],
                         shell=True,
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
        SparkContext._gateway = None
