# Spark ESRI

Project to demonstrate the usage of [Apache Spark](https://spark.apache.org/) within a [Jupyter notebook within ArcGIS Pro](https://pro.arcgis.com/en/pro-app/arcpy/get-started/pro-notebooks.htm).

### Create a new Pro Conda Environment.

Start a `Python Command Prompt`:

![](media/Command.png)

Execute the following commands:

```commandline
conda remove --yes --all --name spark_esri
conda create --yes --name spark_esri --clone arcgispro-py3
activate spark_esri
pip install pyarrow
proswap spark_esri
```

Install the Esri Spark module.

```commandline
git clone https://github.com/mraad/spark-esri.git
cd spark-ersi
python setup.py install
```

Note that the `java_gateway.py` code is borrowed from Spark 3.0 source code. Hoping that this will not be needed when we will switch to 3.0.

### Create a [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html)

```
conda remove --yes --all --name dbconnect
conda create --yes --name dbconnect --clone arcgispro-py3
activate dbconnect
pip install -U databricks-connect==6.6 pyarrow autovizwidget
proswap dbconnect

databricks-connect configure
set SPARK_HOME=c:\users\mraad\appdata\local\esri\conda\envs\dbconnect\lib\site-packages\pyspark
databricks-connect test
```

```
spark.databricks.service.server.enabled true
```

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

## References

- https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
- https://marinecadastre.gov/ais/
