"""A local test system: the k3d cluster of the cluster tests, kept for manual runs.

`up` creates the cluster `simrun-test` exactly as the session of the cluster tests
does (or reuses it), builds and pushes the image of the runs from the working tree
and writes the settings and the tokens of a service on the host into `.tools/k3d/local`.
`down` deletes the cluster, the registry, the store and the settings. The cluster is
the one of `SIMRUN_K3D_KEEP=1`, so the cluster tests and manual runs share it.

    uv run python tests/cluster/local.py up
    uv run python tests/cluster/local.py down
"""

import argparse
import shutil

import yaml
from k3d_cluster import (
    KUBECONFIG,
    REGISTRY,
    REGISTRY_HOST,
    STORE,
    TOOLS,
    apply_manifests,
    build_image,
    check_disk,
    create_cluster,
    delete_cluster,
    download_tools,
    mirror_uv_image,
)

from simrun.testing import TOKENS

LOCAL = TOOLS / "k3d" / "local"
SETTINGS = LOCAL / "settings.yaml"


def up() -> None:
    download_tools()
    check_disk()
    create_cluster()
    apply_manifests()
    image, wheel = build_image()
    mirror_uv_image()
    LOCAL.mkdir(parents=True, exist_ok=True)
    tokens = LOCAL / "tokens.yaml"
    tokens.write_text(yaml.safe_dump(TOKENS))
    settings = {
        "store": str(STORE),
        "db": str(LOCAL / "simrun.sqlite"),
        "tokens": str(tokens),
        "owner": "mk",
        "cluster": {
            "registry": REGISTRY,
            "registry_url": f"http://{REGISTRY_HOST}",
            "ttl_after_finished_s": 600,
        },
        "poll_s": 2.0,
        "default_image": image,
        "simrun_wheel": str(wheel),
        "backend": "simrun.service.argo_backend:backend",
    }
    SETTINGS.write_text(yaml.safe_dump(settings, sort_keys=False))
    print(f"""
The cluster is up. Start the service on the host:

  export KUBECONFIG={KUBECONFIG}
  export SIMRUN_CONFIG={SETTINGS}
  uv run uvicorn --factory simrun.service.app:serve --port 8000

and use it from another shell:

  export SIMRUN_URL=http://localhost:8000
  export SIMRUN_TOKEN=t-mk
  export SIMRUN_STORE={STORE}
  uv run simrun submit examples/squares.yaml
""")


def down() -> None:
    delete_cluster()
    shutil.rmtree(LOCAL, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["up", "down"])
    {"up": up, "down": down}[parser.parse_args().command]()


if __name__ == "__main__":
    main()
