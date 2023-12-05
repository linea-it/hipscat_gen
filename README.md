# Import hipscat on Slurm
The idea of this repository is to store scripts to run import-hipscat with Slurm in the LIneA environment.

## Setup Environment

1. Clone the repository and access the directory:

```bash
git clone https://github.com/linea-it/import_hipscat.git  
cd import_hipscat
```

2. Create environment (using Conda):
   
```bash
conda create -n hipscat python=3.10
conda activate hipscat
pip install --no-cache-dir hipscat-import
pip install --no-cache-dir ray
ulimit -s 50000
```

3. Create configuration file (example used to import DP0 into the LIneA environment - dp0.yml):

```bash
cat dp0.yml
# Hipscat config to DP0
# The idea of yaml is to provide configuration parameters for hipscat.
# Each key must be a valid argument to the ImportArguments class.
# https://hipscat-import.readthedocs.io/en/latest/autoapi/hipscat_import/catalog/arguments/index.html#hipscat_import.catalog.arguments.ImportArguments

id_column: objectId
ra_column: ra
dec_column: dec
input_path: /lustre/t0/scratch/users/singulani/hipscat/inputs_dp0
input_format: parquet
output_catalog_name: DP0
output_path: /lustre/t0/scratch/users/singulani/hipscat_gen/cats
dask_tmp: /lustre/t0/scratch/users/singulani/hipscat_gen/tmp
dask_n_workers: 5
overwrite: true
resume: true
```

## Run using only one node

```bash
sbatch submit.sbatch dp0.yml
```

## Run using Ray/Dask Cluster (with Slurm)

```bash
sbatch submit-ray-cluster.sbatch dp0.yml
```
