"""The cluster of the integration tests: k3d with Kueue, Argo Workflows and BuildKit.

Run with `SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster`. The session creates
the k3d cluster `simrun-test` (one server with the infrastructure, three agents of
6 GiB labelled `simrun/worker=true`) with the registry `k3d-simrun-registry:5000`
(`localhost:5000` from the host), applies `deploy/k3d`, builds the image of the runs
(simrun with the demo plugin, `Dockerfile`) and pushes it, and mirrors the base image
of the images of environments into the registry. Every test starts the service in
its process with an `ArgoBackend` and a database of its own; the store is
`.tools/k3d/store`, mounted into every node. After a test its workflows are deleted
and the reservations removed; after the session the cluster and the registry are
deleted unless `SIMRUN_K3D_KEEP=1`, and a kept cluster is used again.

k3d and kubectl are downloaded into `.tools/bin` (pinned versions); the KUBECONFIG
of the cluster is `.tools/kubeconfig`, so no context of the user is touched.
"""

import os
from collections.abc import Callable, Generator, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from k3d_cluster import (
    KUBECONFIG,
    REGISTRY,
    REGISTRY_HOST,
    STORE,
    Cluster,
    Service,
    apply_manifests,
    build_image,
    check_disk,
    create_cluster,
    delete_cluster,
    download_tools,
    enabled,
    mirror_uv_image,
)
from kubernetes import config as kube_config

from simrun.config import ClusterConfig
from simrun.plugins import TaskType
from simrun.service.app import ServiceSettings, create_app
from simrun.service.argo_backend import ArgoBackend
from simrun.testing import TOKENS


@pytest.fixture(scope="session")
def cluster() -> Iterator[Cluster]:
    """The k3d cluster, created for the session unless it is kept from before."""
    if not enabled():
        pytest.skip("the cluster tests run with SIMRUN_K3D=1")
    download_tools()
    check_disk()
    try:
        # inside: a creation which fails half way leaves nothing behind
        create_cluster()
        apply_manifests()
        image, wheel = build_image()
        mirror_uv_image()
        api = kube_config.new_client_from_config(config_file=str(KUBECONFIG))
        config = ClusterConfig(
            registry=REGISTRY,
            registry_url=f"http://{REGISTRY_HOST}",
            ttl_after_finished_s=600,
        )
        yield Cluster(api, image, wheel, config)
    finally:
        if os.environ.get("SIMRUN_K3D_KEEP") != "1":
            delete_cluster()


@pytest.fixture
def service_factory(
    cluster: Cluster, tmp_path: Path
) -> Callable[..., AbstractContextManager[Service]]:
    """Start the service: `types=` the task types (of the entry points by default),
    other keywords are settings; a database of its own, on the cluster of the session.
    """

    @contextmanager
    def factory(
        types: dict[str, TaskType] | None = None, **kw: Any
    ) -> Generator[Service]:
        tokens = tmp_path / "tokens.yaml"
        tokens.write_text(yaml.safe_dump(TOKENS))
        values: dict[str, Any] = {
            "store": STORE,
            "db": tmp_path / "resources.sqlite",
            "tokens": tokens,
            "owner": "mk",
            "cluster": cluster.config,
            "poll_s": 1.0,
            "default_image": cluster.image,
            "simrun_wheel": cluster.wheel,
        }
        settings = ServiceSettings(**{**values, **kw})
        backend = ArgoBackend(cluster.config, cluster.api)
        try:
            with TestClient(create_app(settings, backend, types)) as http:
                yield Service(http, cluster, settings)
        finally:
            cluster.clean()

    return factory


@pytest.fixture
def service(
    service_factory: Callable[..., AbstractContextManager[Service]],
) -> Iterator[Service]:
    """The service with the task types of the entry points."""
    with service_factory() as svc:
        yield svc
