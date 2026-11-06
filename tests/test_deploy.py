"""The image of the service and its deployment fit together."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
DOCKERFILE = (ROOT / "deploy/service/Dockerfile").read_text()


def _env() -> dict[str, str]:
    """The variables of the ENV instructions (the image at runtime)."""
    env: dict[str, str] = {}
    text = DOCKERFILE.replace("\\\n", " ")
    for line in text.splitlines():
        if line.startswith("ENV "):
            for pair in line.removeprefix("ENV ").split():
                key, _, value = pair.partition("=")
                env[key] = value.strip('"')
    return env


def _deployment() -> dict:
    docs = yaml.safe_load_all((ROOT / "deploy/cluster/simrun.yaml").read_text())
    return next(d for d in docs if d and d["kind"] == "Deployment")


def test_the_service_keeps_the_cache_of_uv() -> None:
    # the service resolves environments with uv at runtime: a cache on a volume,
    # no cache only while the image is built
    env = _env()
    assert "UV_NO_CACHE" not in env
    cache = env["UV_CACHE_DIR"]
    spec = _deployment()["spec"]["template"]["spec"]
    (container,) = spec["containers"]
    mounts = {m["mountPath"]: m["name"] for m in container["volumeMounts"]}
    volume = next(v for v in spec["volumes"] if v["name"] == mounts[cache])
    assert "persistentVolumeClaim" in volume


def test_plugins_are_installed_with_the_lock_of_simrun_as_constraints() -> None:
    text = DOCKERFILE.replace("\\\n", " ")
    (line,) = [line for line in text.splitlines() if "/tmp/plugins/*.whl;" in line]
    assert "--constraint /tmp/constraints.txt" in line
    assert "COPY constraints.txt /tmp/constraints.txt" in DOCKERFILE
