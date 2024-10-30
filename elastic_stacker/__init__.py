from importlib.metadata import version

import fire
from .stacker import Stacker

__version__ = "0.3.1"

def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
