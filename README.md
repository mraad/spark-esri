# Spark ESRI

Project to demonstrate the usage of Apache Spark within a Jupyter notebook within ArcGIS Pro.

### Create a new Pro Conda Environment.

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

Note that the `java_gateway.py` code borrowed from Spark 3.0. Hoping that this will not be needed when we will switch to 3.0.

### Notebooks

- [Spatial Binning](spark_esri.ipynb)

![](media/Notebook.png)

![](media/Pro1.png)

## References

- https://www.esri.com/arcgis-blog/products/arcgis-pro/health/use-proximity-tracing-to-identify-possible-contact-events/
- https://marinecadastre.gov/ais/
