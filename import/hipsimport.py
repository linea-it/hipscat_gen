import hipscat_import.pipeline as runner
from hipscat_import.pipeline import pipeline_with_client
import yaml
import os
from sys import argv
import dask.config
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster

dask.config.set({"array.chunk-size": "512 MiB"})
dask.config.set({"distributed.workers.memory.spill": False})
dask.config.set({"distributed.workers.memory.target": False})
dask.config.set({"distributed.workers.memory.terminate": 0.99})
dask.config.set({"distributed.workers.memory.pause": 0.98})
dask.config.set({"distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_": 100})


def get_config(cname):
    with open(cname, "r", encoding="utf-8") as _file:
        return yaml.safe_load(_file)


def main(cname):
    params = get_config(cname)
    args = runner.ImportArguments(**params)

    executor = os.getenv("EXECUTOR", "local")

    match executor:
        case "local":
            cluster = LocalCluster(
                n_workers=args.dask_n_workers,
                local_directory=args.dask_tmp,
                threads_per_worker=args.dask_threads_per_worker,
            )
        case "slurm":
            pslurm = get_config("../slurm.yml")
            cluster = SLURMCluster(**pslurm)
            cluster.adapt(maximum_jobs=args.dask_n_workers)
        case _:
            cluster = LocalCluster(
                n_workers=1,
                threads_per_worker=1,
            )
            cluster.scale(2)

    with Client(cluster) as client:
        pipeline_with_client(args, client)

    cluster.close()


if __name__ == "__main__":
    if len(argv) > 1:
        CNAME = argv[1]
    else:
        raise ValueError("No config")

    main(CNAME)
