__version__ = "0.3.0"

import fire
from .stacker import Stacker


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
