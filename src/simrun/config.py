"""The configuration of the cluster the runs are submitted to."""

from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

# the base image of the images of the environments: uv with the python of the
# service, pinned by the digest of its index, which the registry mirrors (the tag
# `0.13.7-python3.14-trixie-slim` of `ghcr.io/astral-sh/uv`)
DEFAULT_BASE_IMAGE = (
    "astral-sh/uv:0.13.7-python3.14-trixie-slim"
    "@sha256:e80c05a310400cfc90af00b5b7271ef9ad24816e3265e1162af8a678e1aad099"
)
# about the seconds from the creation of a pod to its first unit on the cluster:
# the admission by Kueue, the init container of Argo, the start of the container
POD_START_S = 10.0
# the name of a Debian package, as `apt-get install` takes it
DebianPackage = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9+.-]+$")]


class ClusterConfig(BaseModel):
    """Where and how the pods of a run are placed in the cluster.

    Attributes:
        namespace: Namespace of the workflows and their pods.
        local_queue: Kueue LocalQueue of the pods.
        store_claim: PersistentVolumeClaim of the NFS run store.
        store_mount: Path the run store is mounted at in a pod.
        scratch_path: Local scratch, a hostPath on the local disk of a node, mounted
            at the same path in a pod.
        registry: Host of the image registry inside the network, as the nodes and
            BuildKit name it in image references.
        registry_url: URL of the API of the registry as the service reaches it,
            `http://<registry>` by default.
        service_account: Service account of the pods of the workflows, allowed to
            write the results of the Argo executor.
        buildkit: Address of the BuildKit daemon the builds of images run on.
        buildctl_image: Image of the job which sends a build to BuildKit.
        base_image: The base image of the images of the environments, an image with
            uv and the python of the service (`ServiceSettings.python`); a
            repository of `registry` (the mirror of an image of another registry)
            unless it names a registry host itself.
        system_packages: The Debian packages the images of the environments install
            on the base image: the shared libraries which wheels link but do not
            bundle, `libexpat1` for python-libsbml.
        grace_s: Seconds a worker has to stop after SIGTERM.
        ttl_after_finished_s: Seconds a finished workflow is kept.
        pod_start_s: About the seconds from the creation of a pod to its first unit;
            a split task gets a further pod only while every pod has at least this
            much work (`simrun.planner.plan_task`).
    """

    namespace: str = "simrun"
    local_queue: str = "simrun"
    store_claim: str = "simrun-store"
    store_mount: str = "/store"
    scratch_path: str = "/scratch"
    registry: str
    registry_url: str | None = None
    service_account: str = "simrun-workflow"
    buildkit: str = "tcp://buildkitd.simrun:1234"
    buildctl_image: str = "moby/buildkit:v0.34.2-rootless"
    base_image: str = DEFAULT_BASE_IMAGE
    system_packages: list[DebianPackage] = ["libexpat1"]
    grace_s: int = 60
    ttl_after_finished_s: int = 86400
    pod_start_s: float = Field(POD_START_S, gt=0)

    def base_image_ref(self) -> str:
        """The base image of the environments as the builds name it.

        `base_image` below `registry`, unless its first component is a registry
        host (it has a `.` or a `:`, or is `localhost`).
        """
        first, slash, _ = self.base_image.partition("/")
        if slash and ("." in first or ":" in first or first == "localhost"):
            return self.base_image
        return f"{self.registry}/{self.base_image}"
