from setuptools import find_packages, setup

setup(
    name="spark_esri",
    version="0.9",
    description="Python Bindings to built-in Spark instance in ArcGIS Pro",
    long_description="Python Bindings to built-in Spark instance in ArcGIS Pro",
    long_description_content_type="text/markdown",
    author="Mansour Raad",
    author_email="mraad@esri.com",
    python_requires=">=3.6",
    packages=find_packages(where="python"),
    package_dir={"": "python"}
)
