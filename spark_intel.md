### Install Intel Dist of Python

```
conda deactivate
conda update conda
conda config --add channels intel
conda remove --yes --all --name spark_intel
conda create --yes --name spark_intel intelpython3_core python=3
source activate spark_intel
conda install --yes -c intel numba pandas
conda install --yes jupyter jupyterlab ipywidgets matplotlib seaborn shapely pyshp pyarrow rtree tqdm
conda install -c conda-forge ipympl nodejs

jupyter labextension install @jupyter-widgets/jupyterlab-manager
```

Install Kite (Optional)

```
pip install jupyter-kite && jupyter labextension install "@kiteco/jupyterlab-kite"
```
