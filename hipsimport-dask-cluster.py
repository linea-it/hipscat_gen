import hipscat_import.pipeline as runner
from hipscat_import.pipeline import pipeline_with_client
import yaml
from sys import argv
import dask.config
from dask.distributed import LocalCluster, Client
from dask_jobqueue import SLURMCluster

dask.config.set({"array.chunk-size": "512 MiB"})
dask.config.set({"distributed.workers.memory.spill": False})
dask.config.set({"distributed.workers.memory.target": False})
dask.config.set({"distributed.workers.memory.terminate": 0.99})
dask.config.set({"distributed.workers.memory.pause": 0.98})
dask.config.set({"distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_": 100})


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
    args = runner.ImportArguments(**params)

    cluster = SLURMCluster(
        cores=args.dask_threads_per_worker,
        processes=1,
        memory="250GiB",
        queue="cpu",
        job_extra_directives=["--propagate", "--time=2:00:00"],
    )

    with Client(cluster) as client:
        cluster.scale(args.dask_n_workers)
        pipeline_with_client(args, client)

    cluster.close()

if __name__ == "__main__": main()
