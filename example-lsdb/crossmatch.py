from dask.distributed import LocalCluster, Client
from dask_jobqueue import SLURMCluster
import os
import lsdb
import yaml
from sys import argv
import dask.config
import dask.distributed

dask.config.set({"array.chunk-size": "512 MiB"})
dask.config.set({"distributed.workers.memory.spill": False})
dask.config.set({"distributed.workers.memory.target": False})
dask.config.set({"distributed.workers.memory.terminate": 0.99})
dask.config.set({"distributed.workers.memory.pause": 0.98})
dask.config.set({"distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_": 100})


def main():
    if len(argv) > 2:
        datapath1 = argv[1]
        datapath2 = argv[2]
    else:
        raise ValueError("No config")

    with open("../slurm.yml", "r", encoding="utf-8") as _file:
        pslurm = yaml.safe_load(_file)

    executor = os.getenv("EXECUTOR", "local")

    match executor:
        case "slurm":
            cluster = SLURMCluster(**pslurm)
        case _:
            cluster = LocalCluster(
                n_workers=1,
                threads_per_worker=1,
            )

    with Client(cluster) as _:
        cluster.scale(2)
        data1 = lsdb.read_hipscat(datapath1)
        data2 = lsdb.read_hipscat(datapath2)
        cross = data1.crossmatch(data2)
        data = cross.compute()
        print(f"Count:\n{data.count()}")
        data.to_parquet("cross.parquet")

    cluster.close()


if __name__ == "__main__":
    main()
