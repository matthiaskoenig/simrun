import re
from pathlib import Path

import simrun

ROOT = Path(__file__).parents[1]
# the files naming the image of the service and the wheel of simrun it carries;
# bump-my-version updates them with the version (`.bumpversion.toml`)
DEPLOYMENT = [
    "deploy/cluster/simrun.yaml",
    "deploy/service/Dockerfile",
    "docs/deployment.md",
]


def test_the_deployment_names_the_version_of_simrun():
    for name in DEPLOYMENT:
        text = (ROOT / name).read_text()
        found = set(re.findall(r"simrun-service:(\d+\.\d+\.\d+)", text))
        found |= set(re.findall(r"simrun-(\d+\.\d+\.\d+)-py3-none-any\.whl", text))
        assert found == {simrun.__version__}, name


def test_bump_my_version_updates_the_deployment():
    config = (ROOT / ".bumpversion.toml").read_text()
    for name in DEPLOYMENT:
        assert f'filename = "./{name}"' in config, name
