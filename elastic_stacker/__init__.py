import fire
from .stacker import Stacker

__version__="0.1.2"


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
