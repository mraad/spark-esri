#
# Code borrowed and modified from https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
#
import os
import subprocess
from typing import Dict

import arcpy
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from spark.java_gateway import launch_gateway

SparkContext._gateway = None


def spark_start(config: Dict = {}) -> SparkSession:
    pro_home = arcpy.GetInstallInfo()["InstallDir"]
    # pro_lib_dir = os.path.join(pro_home, "Java", "lib")
    pro_runtime_dir = os.path.join(pro_home, "Java", "runtime")
    os.environ["HADOOP_HOME"] = os.path.join(pro_runtime_dir, "hadoop")
    conf = SparkConf()
    conf.set("spark.ui.enabled", False)
    conf.set("spark.ui.showConsoleProgress", False)
    conf.set("spark.sql.execution.arrow.enabled", True)
    conf.set("spark.sql.catalogImplementation", "in-memory")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    for k, v in config.items():
        conf.set(k, v)
    #
    # these need to be reset on every run or pyspark will think the Java gateway is still up and running
    os.environ.unsetenv("PYSPARK_GATEWAY_PORT")
    os.environ.unsetenv("PYSPARK_GATEWAY_SECRET")
    SparkContext._jvm = None
    SparkContext._gateway = None

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
    else:
        print("Warning: Undefined variable SparkContext._gateway")
