# simrun

Runner of simulation workflows on a k3s cluster with Argo Workflows and Kueue.

A workflow is a YAML file of tasks which read the outputs of other tasks.

## Deployment

`deploy/cluster` is the cluster as a kustomization: Kueue v0.20.5 and Argo Workflows
v4.1.6 (the release manifests, committed unchanged, so a setup needs no download),
the six priority classes of simrun (each a Kueue `WorkloadPriorityClass` and a
Kubernetes `PriorityClass` of the same value), the queue (a `Topology` by host, a
`ResourceFlavor` over the nodes labelled `simrun/worker=true`, the `ClusterQueue`
`simrun` preempting lower priorities, the `LocalQueue` `simrun`), the service account
of the workflows, the run store on NFS (set the export in
`deploy/cluster/store-settings.yaml`), Zot, BuildKit and the service. Apply it with
`kubectl apply --server-side -k deploy/cluster`; the resources of Kueue are accepted
once its CRDs and its webhook are up, so the first apply may have to be repeated.

`deploy/k3d` is the overlay of the integration tests.

## Integration tests on k3d

```bash
SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster
SIMRUN_K3D=1 SIMRUN_K3D_KEEP=1 uv run pytest -m cluster tests/cluster  # keep the cluster
```

They need docker, about 20 GiB of free disk and the network (the images of k3s,
Kueue, Argo, BuildKit and `python:3.14-slim-bookworm`). The session downloads k3d
and kubectl into `.tools/bin`, creates the k3d cluster `simrun-test` (one server,
tainted, with the infrastructure; three agents of 6 GiB labelled `simrun/worker=true`)
with the registry `k3d-simrun-registry:5000` (`localhost:5000` from the host), writes
its kubeconfig to `.tools/kubeconfig` (the contexts of the user are not touched),
applies `deploy/k3d`, builds the image of the runs from the working tree
(`tests/cluster/Dockerfile`) and pushes it. Each test runs the service in its process
with an `ArgoBackend` and the store `.tools/k3d/store`, which k3d mounts into every
node. At the end the cluster, the registry, the store and the images of the runs
are deleted, unless `SIMRUN_K3D_KEEP=1`; a kept cluster is used by the next session.
The tests run in one worker of pytest-xdist and take about six minutes on a created
cluster.

## Findings on the cluster

The integration tests verified the design on k3s v1.35.5 (k3d v5.9.0) with Kueue
v0.20.5 and Argo Workflows v4.1.6; what differed from the assumptions is in the
manifests and the code:

- **Plain pods of Argo are Kueue workloads.** The queue label of a template reaches
  the pod; Kueue gates it, creates a workload per pod and admits it. An Argo node whose
  pod is gated is `Pending` (its `Retry` node too), so the backend reports it queued.
- **Preemption and the retries of Argo.** Kueue preempts a workload of a lower
  priority (`PreemptedWorkload`) by deleting its pod, within the grace period: the
  worker stops on SIGTERM, releases its claims and writes its report. Argo marks the
  attempt `Error` with the message `pod deleted` and no exit code
  (`lastRetry.exitCode` is `"-1"`), so the retry strategy retries it; the new pod is
  gated again and waits. The backend reports such a node `Preempted` until its next
  pod runs. Workloads of equal priority never preempt each other, so the extra
  workers of two runs of one priority wait for each other's end.
- **Topology aware scheduling.** A `ResourceFlavor` with a topology needs node labels
  (`at least one nodeLabel is required when topology is set`), hence the label
  `simrun/worker=true` of the nodes of the runs, which `ArgoBackend.nodes` lists too.
  On admission Kueue adds the host and the flavor labels to the node selector of the
  pod; it honours the node selector and the toleration of a reserved run, and the
  taint `NoExecute` of a reservation keeps the pods of other runs off the node. Kueue
  preempts for the capacity of a node as well: with a quota far above the cluster a
  run of a higher priority still evicted a pod of a lower one from a full node.
- **The executor counts.** Kueue counts every container, so the init and wait
  containers of Argo request what they limit (`executor.resources` in the controller
  config). The init container copies the argoexec binary (about 150 MB) into the pod
  and with 128Mi it was killed for memory now and then (`init: OOMKilled`); it has
  256Mi. `ArgoBackend.nodes` subtracts one executor container from the allocatable
  resources and rounds down to whole cores and GiB, so a pod the planner sizes for a
  node fits it.
- **Out of memory.** The kernel kills the whole container (cgroup v2,
  `memory.oom.group`), so the worker writes no report; Argo v4 names the container in
  the message, `main: OOMKilled (exit code 137)`, which the backend counts per worker.
  The `podSpecPatch` of the retry (`lastRetry.exitCode == '137'`) gives the next
  attempt the retry memory and memory per process, as designed.
- **Stopping a workflow.** After `shutdown: Stop` Argo fails the nodes still pending
  (`workflow shutdown with strategy: Stop`); the backend deletes their gated pods.
- **Logs.** The logs of an attempt whose pod Kueue deleted are gone with the pod.
- **Priority classes.** The pods name a Kubernetes `PriorityClass`, which must exist;
  it has `preemptionPolicy: Never`, so only Kueue preempts.
- **Registry and builds.** The nodes and BuildKit name the registry as
  `ClusterConfig.registry`, the service reaches its API at
  `ClusterConfig.registry_url`. BuildKit runs rootless (seccomp and AppArmor
  unconfined, `--oci-worker-no-process-sandbox`); a build is a Job of `buildctl` whose
  context is a ConfigMap (at most 1 MB: the Dockerfile and the simrun wheel).
