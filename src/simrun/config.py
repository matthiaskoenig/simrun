"""The configuration of the cluster the runs are submitted to."""

from pydantic import BaseModel


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
        grace_s: Seconds a worker has to stop after SIGTERM.
        ttl_after_finished_s: Seconds a finished workflow is kept.
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
    grace_s: int = 60
    ttl_after_finished_s: int = 86400
