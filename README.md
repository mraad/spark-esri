# Spark ESRI

Project to demonstrate the usage of [Apache Spark](https://spark.apache.org/) within
a [Jupyter notebook within ArcGIS Pro](https://pro.arcgis.com/en/pro-app/arcpy/get-started/pro-notebooks.htm).

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
conda remove --yes --all --name spark_esri
conda create --yes --name spark_esri --clone arcgispro-py3

proswap spark_esri

conda install --yes -c conda-forge -c esri -c default^
    numba=0.52.0^
    pandas=1.1.5^
    untangle=1.1.1^
    pyodbc=4.0.30^


pip install pyarrow==2.0.0

# To use s3://
conda install --yes -c conda-forge -c esri -c default boto3=1.16.33 s3fs=0.4.2

# To use gs://
conda install --yes -c conda-forge -c esri -c default gcsfs=0.7.1
```

Install the Esri Spark module.

**Note**: You _might_ need to install [Git for Windows](https://gitforwindows.org/).

```commandline
git clone https://github.com/mraad/spark-esri.git
cd spark-esri
python setup.py install
```

### [Spatial Binning](spark_esri.ipynb) Notebook

![](media/Notebook.png)

![](media/Pro1.png)

### [MicroPathing](micro_path.ipynb) Notebook

![](media/Micropath1.png)

Please note the usage of
the [range slider](https://pro.arcgis.com/en/pro-app/help/mapping/range/get-started-with-the-range-slider.htm) on the
map to filter the micropaths between a user defined hour of day.

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

## My Notes:

```
# to use @pandas_udf, we have to downgrade pyarrow :-(
pip install pyarrow==0.8
```

## References

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
- https://gist.github.com/rkaneko/dd2fae35149a29405d5e287ccd62677f Put parquet file on MinIO (S3 compatible storage)
  using pyarrow and s3fs