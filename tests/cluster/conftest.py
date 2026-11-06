"""The cluster of the integration tests: k3d with Kueue, Argo Workflows and BuildKit.

Run with `SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster`. The session creates
the k3d cluster `simrun-test` (one server tainted for the runs, three agents of
6 GiB labelled `simrun/worker=true`) with the registry `k3d-simrun-registry:5000`
(`localhost:5000` from the host), applies `deploy/k3d`, builds the image of the runs
(simrun with the demo plugin, `Dockerfile`) and pushes it. Every test starts the
service in its process with an `ArgoBackend` and a database of its own; the store is
`.tools/k3d/store`, mounted into every node. After a test its workflows are deleted
and the reservations removed; after the session the cluster and the registry are
deleted unless `SIMRUN_K3D_KEEP=1`, and a kept cluster is used again.

k3d and kubectl are downloaded into `.tools/bin` (pinned versions); the KUBECONFIG
of the cluster is `.tools/kubeconfig`, so no context of the user is touched.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from k3d_cluster import (
    KUBECONFIG,
    REGISTRY,
    REGISTRY_HOST,
    STORE,
    TOKENS,
    Cluster,
    Service,
    apply_manifests,
    build_image,
    check_disk,
    create_cluster,
    delete_cluster,
    download_tools,
    enabled,
)
from kubernetes import config as kube_config

from simrun.config import ClusterConfig
from simrun.service.app import ServiceSettings, create_app
from simrun.service.argo_backend import ArgoBackend


@pytest.fixture(scope="session")
def cluster() -> Iterator[Cluster]:
    """The k3d cluster, created for the session unless it is kept from before."""
    if not enabled():
        pytest.skip("the cluster tests run with SIMRUN_K3D=1")
    download_tools()
    check_disk()
    create_cluster()
    try:
        apply_manifests()
        image = build_image()
        api = kube_config.new_client_from_config(config_file=str(KUBECONFIG))
        config = ClusterConfig(
            registry=REGISTRY,
            registry_url=f"http://{REGISTRY_HOST}",
            ttl_after_finished_s=600,
        )
        yield Cluster(api, image, config)
    finally:
        if os.environ.get("SIMRUN_K3D_KEEP") != "1":
            delete_cluster()


@pytest.fixture
def service(cluster: Cluster, tmp_path: Path) -> Iterator[Service]:
    """The service with a database of its own, on the cluster of the session."""
    tokens = tmp_path / "tokens.yaml"
    tokens.write_text(yaml.safe_dump(TOKENS))
    settings = ServiceSettings(
        store=STORE,
        db=tmp_path / "resources.sqlite",
        tokens=tokens,
        owner="mk",
        cluster=cluster.config,
        poll_s=1.0,
        default_image=cluster.image,
    )
    backend = ArgoBackend(cluster.config, cluster.api)
    try:
        with TestClient(create_app(settings, backend)) as http:
            yield Service(http, cluster, settings)
    finally:
        cluster.clean()
