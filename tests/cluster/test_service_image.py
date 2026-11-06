"""The image of the service from `deploy/service/Dockerfile`, built once.

The image serves the API with the settings of a file (`SIMRUN_CONFIG`), has uv, the
simrun wheel at the path the settings name and the task types of a plugin installed
from its wheel.
"""

import json
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import yaml
from k3d_cluster import Cluster, build_service_image

from simrun.testing import TOKENS

pytestmark = [
    pytest.mark.cluster,
    pytest.mark.skipif(
        os.environ.get("SIMRUN_K3D") != "1",
        reason="the cluster tests run with SIMRUN_K3D=1",
    ),
]

PLUGIN = """
from pydantic import BaseModel

from simrun.plugins import TaskType


class EchoSettings(BaseModel):
    text: str


ECHO = TaskType(name="test.echo", settings=EchoSettings, command="echo {settings}")
"""


def _plugin_wheel(tmp: Path) -> Path:
    """A plugin with one task type, as a wheel."""
    root = tmp / "plugin"
    (root / "src" / "echo_plugin").mkdir(parents=True)
    (root / "src" / "echo_plugin" / "__init__.py").write_text(PLUGIN)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "echo-plugin"\nversion = "0.1.0"\n'
        'requires-python = ">=3.14"\ndependencies = []\n'
        '[project.entry-points."simrun.task_types"]\n'
        '"test.echo" = "echo_plugin:ECHO"\n'
        '[build-system]\nrequires = ["uv_build>=0.13.7,<0.14.0"]\n'
        'build-backend = "uv_build"\n'
    )
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp / "dist"), str(root)],
        check=True,
        capture_output=True,
    )
    (wheel,) = (tmp / "dist").glob("*.whl")
    return wheel


@pytest.fixture(scope="session")
def service_image(cluster: Cluster, tmp_path_factory: pytest.TempPathFactory) -> str:
    """The image of the service with the plugin, built once per session."""
    plugin = _plugin_wheel(tmp_path_factory.mktemp("plugin"))
    return build_service_image(cluster.wheel, [plugin])


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def container(service_image: str, cluster: Cluster, tmp_path: Path) -> Iterator[str]:
    """The service of the image in a container; yields its URL."""
    config = tmp_path / "config"
    config.mkdir()
    (config / "tokens.yaml").write_text(yaml.safe_dump(TOKENS))
    settings = {
        "store": "/store",
        "db": "/var/lib/simrun/resources.sqlite",
        "tokens": "/etc/simrun/tokens.yaml",
        "owner": "mk",
        "simrun_wheel": f"/opt/simrun/{cluster.wheel.name}",
        # a backend in memory: the image is tested, not the cluster
        "backend": "simrun.testing:FakeBackend",
        "default_image": cluster.image,
        "cluster": {"registry": "localhost:5000"},
    }
    (config / "settings.yaml").write_text(yaml.safe_dump(settings))
    config.chmod(0o755)
    port = _free_port()
    name = f"simrun-service-test-{port}"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            f"127.0.0.1:{port}:8000",
            "--volume",
            f"{config / 'settings.yaml'}:/etc/simrun/settings.yaml:ro",
            "--volume",
            f"{config / 'tokens.yaml'}:/etc/simrun/tokens.yaml:ro",
            "--tmpfs",
            "/store",
            "--env",
            "SIMRUN_CONFIG=/etc/simrun/settings.yaml",
            service_image,
        ],
        check=True,
        capture_output=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                httpx.get(f"{url}/runs", timeout=2)
                break
            except httpx.TransportError:
                if time.monotonic() > deadline:
                    logs = subprocess.run(
                        ["docker", "logs", name], capture_output=True, text=True
                    )
                    raise AssertionError(logs.stdout + logs.stderr) from None
                time.sleep(0.5)
        yield url
    finally:
        subprocess.run(["docker", "rm", "--force", name], capture_output=True)


def _in_image(image: str, code: str) -> str:
    done = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "python", image, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout


def test_the_image_has_uv_the_wheel_and_the_plugins(
    service_image: str, cluster: Cluster
) -> None:
    code = (
        "import json, pathlib, shutil, simrun\n"
        "from simrun.plugins import load_task_types\n"
        "print(json.dumps({'uv': shutil.which('uv'), 'version': simrun.__version__,"
        f" 'wheel': pathlib.Path('/opt/simrun/{cluster.wheel.name}').is_file(),"
        " 'types': sorted(load_task_types())}))"
    )
    found = json.loads(_in_image(service_image, code))
    assert found["uv"] is not None
    assert found["wheel"] is True
    assert {"command", "demo.squares", "test.echo"} <= set(found["types"])


def test_the_image_serves_the_api(container: str) -> None:
    headers = {"Authorization": "Bearer t-mk"}
    assert httpx.get(f"{container}/runs", headers=headers).json() == []
    workflow = {
        "name": "echo",
        "tasks": [{"id": "e", "type": "test.echo", "settings": {"text": "hi"}}],
    }
    result = httpx.post(f"{container}/validate", json=workflow, headers=headers)
    assert result.json() == {"ok": True, "errors": []}
    workflow["tasks"][0]["settings"] = {"txt": "hi"}
    result = httpx.post(f"{container}/validate", json=workflow, headers=headers)
    assert "unknown setting 'txt'" in result.json()["errors"][0]
