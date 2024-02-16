import hipscat_import.pipeline as runner
from hipscat_import.pipeline import pipeline_with_client
from dask.distributed import LocalCluster, Client
from dask_jobqueue import SLURMCluster
import yaml
from sys import argv
import dask.config
import dask.distributed


def get_config():
    if len(argv) > 1:
        config_file = argv[1]
    else:
        raise ValueError("No config")

    with open(config_file, 'r') as _file:
        params = yaml.safe_load(_file)

    return params


def main():
    params = get_config()

    dask.config.set({"array.chunk-size": "128 MiB"})
    dask.config.set({"distributed.workers.memory.spill": 0.90})
    dask.config.set({"distributed.workers.memory.target": 0.80})
    dask.config.set({"distributed.workers.memory.terminate": 0.98})

    args = runner.ImportArguments(**params)

    memlim = "%iGB" % round(120 / int(args.dask_n_workers))

    cluster = SLURMCluster(cores=args.dask_n_workers,
                       processes=1,
                       memory=memlim,
                       walltime="04:00:00",
                       queue="cpu")

    with Client(cluster) as client:
        pipeline_with_client(args, client)


if __name__ == "__main__": main()
