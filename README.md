
The idea of this repository is to store scripts to import datasets into hipscat format and perform cross matching using LSDB with Slurm in the LIneA environment.


## Development environment

Thinking of a way to advance the development of this repository, we created two small datasets (datasetA.parquet and datasetB.parquet) to test the import into the hipscat format and the cross match.

The step-by-step process below will be executed locally and starts from creating the environment, going through the import of the data sets and ending with the crossing between them.

1. Clone the repository and access the directory:

```bash
git clone https://github.com/linea-it/slurm_lsdb.git  
cd slurm_lsdb
```

2. Create environment (using Conda):
   
```bash
conda env create -f environment.yml
source env.sh
```

3. Import of test datasets to hipscat:

```bash
cd import
python hipsimport.py datasetA.yml
python hipsimport.py datasetB.yml
cd ..
```

The command above will import the datasets into the `data-sample/hipscat` directory


4. Run cross match:
*Obs: This cross match code is just an example of how to do it*

```bash
cd example-lsdb
python crossmatch.py ../data-sample/hipscat/DatasetA ../data-sample/hipscat/DatasetB
cd ..
```

## Use in Slurm

For use in the Slurm, the only file that must be created is a yaml configuration file with the parameters of the [ImportArguments](https://hipscat-import.readthedocs.io/en/latest/autoapi/hipscat_import/catalog/arguments/index.html#hipscat_import.catalog.arguments.ImportArguments) class (by hipscat_import) referring to the data set that must be imported (path to the input data, directory of output, etc...). For example:

```yaml
sort_columns: id
ra_column: ra
dec_column: dec
input_path: ../data-sample/raw/A
input_format: parquet
output_artifact_name: DatasetA
output_path: ../data-sample/hipscat/
dask_n_workers: 10
dask_threads_per_worker: 56
overwrite: true
resume: true
```

And the execution in slurm would look something like this:

```bash
sbatch submit.sh your.config.file.yml
```