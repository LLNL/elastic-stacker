from importlib.metadata import version

import fire
from .stacker import Stacker

__version__ = version("elastic-stacker")


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
