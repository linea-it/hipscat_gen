from dask.distributed import LocalCluster, Client
from dask_jobqueue import SLURMCluster
import lsdb
import dask.config
import dask.distributed

dask.config.set({"array.chunk-size": "512 MiB"})
dask.config.set({"distributed.workers.memory.spill": False})
dask.config.set({"distributed.workers.memory.target": False})
dask.config.set({"distributed.workers.memory.terminate": 0.99})
dask.config.set({"distributed.workers.memory.pause": 0.98})
dask.config.set({"distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_": 100})


def main():

    cluster = SLURMCluster(
        cores=56,
        processes=1,
        memory="250GiB",
        queue="cpu",
        job_extra_directives=["--propagate", "--time=2:00:00"],
    )

    with Client(cluster) as client:
        cluster.scale(10)
        phot_dp0 = lsdb.read_hipscat('/lustre/t1/tmp/singulani/import_hipscat/outputs/DP02/')
        spec_dp0 = lsdb.read_hipscat('/lustre/t1/tmp/singulani/import_hipscat/outputs/SpeczDP02/')
        cross = spec_dp0.crossmatch(phot_dp0)
        data = cross.compute()
        data.to_parquet('cross-specz-dp02.parquet')

    cluster.close()


if __name__ == "__main__": main()
