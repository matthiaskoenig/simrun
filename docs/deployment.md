# Cluster deployment

`simrun` runs on a k3s cluster of dedicated Linux servers. `deploy/cluster` is the whole cluster as a kustomization:

| component | file | role |
| --- | --- | --- |
| namespace | `namespace.yaml` | the namespace `simrun` of the service, the workflows and their pods |
| priorities | `priority-classes.yaml` | the six priority classes of simrun, each a Kueue `WorkloadPriorityClass` and a Kubernetes `PriorityClass` of the same value, see [Priorities and preemption](concepts.md#priorities-and-preemption) |
| Kueue v0.20.5 | `kueue/` | the release manifests, committed unchanged, and the queue: a `Topology` by host, a `ResourceFlavor` over the nodes labelled `simrun/worker=true`, the `ClusterQueue` `simrun`, which preempts lower priorities, and the `LocalQueue` `simrun` |
| Argo Workflows v4.1.6 | `argo/` | the release manifests of a namespace install, committed unchanged, the configuration of the controller, its requeue time of 1 s instead of 10 s (`requeue.yaml`, so a run shows running soon after its pods start and the next task of the DAG starts about a second after the one before it) and the service account `simrun-workflow` of the pods |
| run store | `store-settings.yaml`, `store.yaml` | the run store on NFS (csi-driver-nfs), mounted by the service and every pod with `nfsvers=4.2` and `lookupcache=positive` |
| registry | `zot.yaml` | [Zot](https://zotregistry.dev), the image registry inside the network |
| builds | `buildkit.yaml` | the BuildKit daemon, which builds the images of the environments, and its state (`buildkitd-state`, 50 GiB of `local-path`): the layers and the downloads of uv of the builds, kept when its pod restarts |
| service | `simrun.yaml` | the service: its settings, its tokens, its volumes and its rights |

Since the release manifests are committed, a setup needs no download. Apply it with

```bash
kubectl apply --server-side -k deploy/cluster
```

The resources of Kueue are accepted once its CRDs and its webhook are up, so the first apply may have to be repeated.

The store is mounted with `lookupcache=positive`: the NFS client caches no negative lookups, so a file a pod wrote, e.g. the marker `out/.simrun-merged` of a merge, is seen by the service at once even when it looked for the file before. The mount options are the ones of the StorageClass when the volume of the claim was provisioned: an existing PersistentVolume keeps its options, so add the option to `spec.mountOptions` of that PersistentVolume (or provision it again) and restart the pods which mount it. Without the option the service waits up to two minutes for the marker of a merge before it fails the task.

## Nodes

The nodes of the runs carry the label `simrun/worker=true`, which the `ResourceFlavor` of Kueue needs (a flavor with a topology needs node labels) and by which the backend lists the nodes:

```bash
kubectl label node <node> simrun/worker=true
```

The infrastructure (the controllers of Argo and Kueue, BuildKit and its build Jobs, Zot, the service) has a node affinity off these nodes (`off-workers.yaml`), so Kueue and the planner count their resources as free for the runs. The backend subtracts from the allocatable resources of a node the wait container of the Argo executor and the requests of the pods which are no pods of runs (the DaemonSets of the cluster, e.g. of the NFS driver and of the monitoring), rounded down to whole cores and GiB, so a pod the planner sizes for a node fits it. The local scratch of a pod is a hostPath on the local disk of its node, `/scratch` by default.

## The values a setup fills in

The manifests carry the values of the cluster of the design. A setup replaces them:

| value | where | meaning |
| --- | --- | --- |
| `server: storage.cluster.internal`, `share: /tank/simrun` | `store-settings.yaml` | the NFS export of the run store, substituted into the StorageClass by the replacements of the kustomization; NFS v4.1 or newer, the ledger relies on its atomic renames |
| `registry.cluster.internal:5000` | `zot.yaml`, `buildkit.yaml`, `simrun.yaml`, `deploy/service/Dockerfile` | the address of the registry as the nodes and BuildKit name it |
| `owner: mk` | `simrun.yaml`, ConfigMap `simrun-config` | the person who owns the cluster |
| `{}` | `simrun.yaml`, Secret `simrun-tokens` | the tokens of the people and agents, see [Authentication](service.md#authentication) |
| `simrun-service:0.3.0`, `simrun-0.3.0-py3-none-any.whl` | `simrun.yaml` | the image of the service and the wheel of simrun it carries, see below |
| `nominalQuota` of cpu, memory, GPUs and ephemeral storage | `kueue/queues.yaml` | the allocatable resources of the worker nodes |

The Service `simrun` (port 80) is reached by the clients, e.g. through an ingress or a DNS name of the network; the examples of this documentation use `http://simrun.cluster.internal`. The development server of the agents mounts the run store, e.g. at `/mnt/cluster`, which is `SIMRUN_STORE` of the clients.

## The service image

`deploy/service/Dockerfile` is the image `simrun-service` of `deploy/cluster/simrun.yaml`: uv (the service resolves the environments of the runs), simrun with the dependencies of its lock, its wheel at `/opt/simrun/` (the `simrun_wheel` of the settings), the plugins and the [web app](web-app.md) at `/opt/simrun/web` (the `web` of the settings, served at `/ui/`). Build it from a context of the requirements of the lock, the wheel, the wheels of the plugins and the built web app (node 24):

```bash
mkdir -p build/service/plugins
uv build --wheel --out-dir build/service
uv export --frozen --no-dev --no-emit-project --no-header --no-annotate \
    > build/service/requirements.txt
uv export --frozen --no-dev --no-emit-project --no-header --no-annotate --no-hashes \
    > build/service/constraints.txt
cp ~/git/sbmlsim/dist/sbmlsim-*.whl build/service/plugins/   # the plugins
(cd frontend && npm ci && npm run build)                     # the web app
rm -rf build/service/web && cp -r frontend/dist build/service/web
cp deploy/service/Dockerfile build/service/
docker build -t registry.cluster.internal:5000/simrun-service:0.3.0 \
    --build-arg WHEEL=simrun-0.3.0-py3-none-any.whl build/service
docker push registry.cluster.internal:5000/simrun-service:0.3.0
```

The plugins are installed with their dependencies at the versions of the lock of simrun (`constraints.txt`): a plugin which needs another version of a dependency of simrun fails the build instead of moving it. The base (`--build-arg BASE`) is the base image of the environments in the registry, which the registry mirrors with its digest (`ClusterConfig.base_image`). At runtime the service keeps the cache of uv in `/var/cache/uv`, a volume of the deployment, so a submission does not download the metadata and the build backends again.

A client without the store mounted (`SIMRUN_STORE`) uploads its snapshots through the API, and the service spools an upload to its temporary directory (`TMPDIR`, else `/tmp`) before it moves the files into the store: the container's `/tmp` must hold up to `max_upload_bytes` (2 GiB by default) per concurrent upload, so give the pod the ephemeral storage for it or mount a volume at `/tmp`.

## Builds of the images of the environments

An image is built by a Job of `buildctl`, which sends the build context (a ConfigMap of its files, owned by the Job) to the BuildKit daemon of the cluster and pushes the image to the registry with the credentials of the optional Secret `simrun-registry-auth`; whether the registry has an image is asked its API.

## The k3d overlay

`deploy/k3d` is the overlay of the [integration tests](testing.md#tests-on-a-k3d-cluster): the store is a hostPath which k3d mounts from the host into every node, the registry is the one of k3d (`k3d-simrun-registry:5000`, no Zot), the service runs in the process of the tests (the Deployment has no replica), the infrastructure runs on the server node, which has no label `simrun/worker`, and the quotas are the allocatable resources of the three agents.

## Findings on the cluster

The integration tests verified the design on k3s v1.35.5 (k3d v5.9.0) with Kueue v0.20.5 and Argo Workflows v4.1.6; what differed from the assumptions is in the manifests and the code:

- **Plain pods of Argo are Kueue workloads.** The queue label of a template reaches the pod; Kueue gates it, creates a workload per pod and admits it. An Argo node whose pod is gated is `Pending` (its `Retry` node too), so the backend reports it queued.
- **Preemption and the retries of Argo.** Kueue preempts a workload of a lower priority by deleting its pod within the grace period: the worker stops on SIGTERM, releases its claims and writes its report. Argo marks the attempt `Error` with the message `pod deleted` and no exit code, so the retry strategy retries it; the new pod is gated again and waits. The backend reports such a node `Preempted` until its next pod runs (a node whose pod failed otherwise, a crash or a kill after its grace period, is `Retrying`). A pod preempted before Argo saw it running is created again by Argo as the same attempt, without a retry, so such a node is reported queued and not `Preempted`.
- **Topology aware scheduling.** A `ResourceFlavor` with a topology needs node labels, hence the label `simrun/worker=true`. On admission Kueue adds the host and the flavor labels to the node selector of the pod; it honours the node selector and the toleration of a reserved run, and the taint `NoExecute` of a reservation keeps the pods of other runs off the node. Kueue preempts for the capacity of a node as well.
- **The executor counts.** Kueue counts every container, so the init and wait containers of Argo request what they limit (`executor.resources` in the controller config). The init container copies the argoexec binary (about 150 MB) into the pod and has 256Mi; with 128Mi it was killed for memory now and then.
- **Out of memory.** The kernel kills the whole container (cgroup v2, `memory.oom.group`), so the worker writes no report; Argo names the container in the message, `main: OOMKilled (exit code 137)`. A kill for memory needs this evidence or a report which counts the kills of the out of memory killer in its cgroup; an exit 137 without either is a pod killed after its grace period, an eviction. Only a pod which was stopped is retried: without an exit code (a lost node, a pod deleted before it ran), killed (137) or stopped on SIGTERM (143); any other exit fails its task at once, and the message of the run names the pod and its exit. The retry limit of a pod is 100: every eviction by Kueue counts against it.
- **The retry memory after a preemption.** Argo's expressions of a retry know only the last attempt (`lastRetry`), so an attempt after one which was preempted following a kill for memory gets the first memory again, not the retry memory. The controller does not count a kill of such an attempt toward the limit; the attempt after it has the retry memory again.
- **Stopping a workflow.** After `shutdown: Stop` Argo fails the nodes still pending; the backend deletes their gated pods.
- **Logs.** The logs of an attempt whose pod Kueue deleted are gone with the pod; the controller keeps the logs of the pods of a task in the store once the task is final.
