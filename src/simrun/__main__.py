"""`python -m simrun`: the command line, as the console script `simrun` runs it."""

from simrun.cli import app

if __name__ == "__main__":  # not on an import, e.g. of every module
    app(prog_name="simrun")
