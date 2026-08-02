from setuptools import find_packages, setup

setup(
    name="spark_esri",
    version="0.12",
    description="Python Bindings to built-in Spark instance in ArcGIS Pro",
    long_description="Python Bindings to built-in Spark instance in ArcGIS Pro",
    long_description_content_type="text/markdown",
    author="Mansour Raad",
    author_email="mraad@esri.com",
    python_requires=">=3.10",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    # pyspark is deliberately NOT a hard dependency - by default this package drives the
    # Spark that ships with ArcGIS Pro. Install this extra only when you need to override it
    # via SPARK_HOME, e.g. to pick up the SPARK-53759 python-worker fix.
    extras_require={"standalone": ["pyspark>=4.1.2"]}
)
