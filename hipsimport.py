import hipscat_import.pipeline as runner
import yaml
from sys import argv


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
    runner.pipeline(args)


if __name__ == "__main__": main()
