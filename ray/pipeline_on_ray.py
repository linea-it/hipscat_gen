import ray
from dask.distributed import Client
from ray.util.dask import disable_dask_on_ray, enable_dask_on_ray
from hipscat_import.pipeline import pipeline_with_client, ImportArguments
from sys import argv
import time
import yaml
import os


def get_config():
    if len(argv) > 1:
        config_file = argv[1]
    else:
        raise ValueError("No config")

    with open(config_file, 'r') as _file:
        params = yaml.safe_load(_file)

    return params


def main():
    start_time = time.time()
    params = get_config()
    args = ImportArguments(**params)

    node_ip = os.getenv('ip_head', None)
    redis_password = os.getenv('redis_password', None)

    print("HEAD IP: ", node_ip)

    if not node_ip:
        raise ValueError("node IP not specified in $ip_head environment variable")

    with ray.init(address=node_ip):
        enable_dask_on_ray()

        with Client(node_ip) as client:
            pipeline_with_client(args, client)

        disable_dask_on_ray()

    print(f"exectime: {round(time.time() - start_time, 2) } sec")


if __name__ == "__main__": main()
