#
# Code borrowed and modified from https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
#
import os
import subprocess
import sys
from typing import Dict

import arcpy

pro_home = arcpy.GetInstallInfo()["InstallDir"]
pro_lib_dir = os.path.join(pro_home, "Java", "lib")
pro_runtime_dir = os.path.join(pro_home, "Java", "runtime")
spark_home = os.path.join(pro_runtime_dir, "spark")

# add spark/py4j libraries from Pro runtime to path for import
sys.path.insert(0, os.path.join(spark_home, "python", "lib", "pyspark.zip"))
sys.path.insert(0, os.path.join(spark_home, "python", "lib", "py4j-0.10.7-src.zip"))

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from spark.java_gateway import launch_gateway

SparkContext._gateway = None


def spark_start(config: Dict = {}) -> SparkSession:
    os.environ["JAVA_HOME"] = os.path.join(pro_runtime_dir, "jre")
    os.environ["HADOOP_HOME"] = os.path.join(pro_runtime_dir, "hadoop")
    os.environ["SPARK_HOME"] = spark_home
    # Get the active conda env.
    if os.getenv('LOCALAPPDATA'):
        pro_env_txt = os.path.join(os.getenv('LOCALAPPDATA'), 'ESRI', 'conda', 'envs', 'proenv.txt')
        if os.path.exists(pro_env_txt):
            with open(pro_env_txt, "r") as fp:
                python_path = fp.read().strip()
                os.environ["PYSPARK_PYTHON"] = os.path.join(python_path, "python.exe")
        else:
            os.environ["PYSPARK_PYTHON"] = os.path.join(pro_home, "bin", "Python", "envs", "arcgispro-py3",
                                                        "python.exe")
    else:
        os.environ["PYSPARK_PYTHON"] = os.path.join(pro_home, "bin", "Python", "envs", "arcgispro-py3", "python.exe")
    #
    # these need to be reset on every run or pyspark will think the Java gateway is still up and running
    os.environ.unsetenv("PYSPARK_GATEWAY_PORT")
    os.environ.unsetenv("PYSPARK_GATEWAY_SECRET")
    SparkContext._jvm = None
    SparkContext._gateway = None

    popen_kwargs = {
        'stdout': subprocess.DEVNULL,  # need to redirect stdout & stderr when running in Pro or JVM fails immediately
        'stderr': subprocess.DEVNULL,
        'shell': True  # keeps the command-line window from showing
    }

    spark_jars = [
        # os.path.join(pro_lib_dir, "spark-desktop-engine.jar"),
        # os.path.join(pro_lib_dir, "arcobjects.jar")
    ]
    spark_jars = ",".join(spark_jars)

    conf = SparkConf()
    conf.set("spark.master", "local[*]")
    conf.set("spark.ui.enabled", False)
    conf.set("spark.ui.showConsoleProgress", False)
    conf.set("spark.sql.execution.arrow.enabled", True)
    conf.set("spark.sql.catalogImplementation", "in-memory")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.jars", spark_jars)
    # Add/Update user defined spark configurations.
    for k, v in config.items():
        if k == "spark.jars":
            v = spark_jars + "," + v
        conf.set(k, v)

    # we have to manage the py4j gateway ourselves so that we can control the JVM process
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
