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

The infrastructure (the controllers, BuildKit and its build Jobs, Zot, the service)
has a node affinity off the nodes labelled `simrun/worker=true`, whose resources Kueue
and the planner count as free for the runs.

`deploy/k3d` is the overlay of the integration tests.

## Pods and their harness

A split task runs `simrun-worker` in its pods and `simrun-merge` in its merge; an
unsplit task of a plugin type runs its command through `simrun-exec`, which installs
the snapshots of the run, stages the inputs, sets `OMP_NUM_THREADS`,
`OPENBLAS_NUM_THREADS` and `MKL_NUM_THREADS` to 1, passes a SIGTERM on to the
command and writes a report of its memory, by the task id. Its exit codes: 0 done,
3 the command failed (not retried), 4 a configuration error, 137 the command was
killed (retried with the retry memory), 143 stopped on SIGTERM (retried). An unsplit
plugin task is sized as one process (one core, `base + per_worker`), unless its type
sets `uses_cores`, then it takes the processes a node allows like a worker. A task of
the built-in type `command` runs its argv as given, in its own image.

## Learned resources

A pod requests `base + per_worker x processes` of memory, limit equal to request.
Every worker (and every unsplit plugin task, see below) writes a report with the
largest sample of the anonymous memory of its cgroup (`memory.stat` `anon`, sampled
every second by its heartbeat thread); `memory.peak` is not used, since it counts
the page cache of staging, installing and writing outputs, which the kernel
reclaims at the limit instead of killing. A worker counts as out of memory only
when the backend reports its pod killed for memory. A request is resolved from the
last 20 observations of its key (task type, model): the 95th percentile of the
measured memory per process times 1.1, and at least 1.5 times the memory per
process of a pod killed for memory, until a later run of the key succeeded.

## Integration tests on k3d

```bash
SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster
SIMRUN_K3D=1 SIMRUN_K3D_KEEP=1 uv run pytest -m cluster tests/cluster  # keep the cluster
```

They need docker, about 20 GiB of free disk and the network (the images of k3s,
Kueue, Argo, BuildKit and `python:3.14-slim-bookworm`). The session downloads k3d
and kubectl into `.tools/bin`, creates the k3d cluster `simrun-test` (one server,
with the infrastructure; three agents of 6 GiB labelled `simrun/worker=true`)
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
  workers of two runs of one priority wait for each other's end. A pod preempted
  before Argo saw it running is created again by Argo as the same attempt, without a
  retry, so such a node is reported queued and not `Preempted`; the take-back test
  checks the `PreemptedWorkload` events of Kueue instead.
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
  A worker which exits 137 itself because its pool lost a process (Ruling 7) counts
  as a kill for memory too, and so does the kill of an unsplit task (by its task id)
  or a merge (`<task>-merge`). The `podSpecPatch` of the retry
  (`lastRetry.exitCode == '137'`) gives the next attempt the retry memory and memory
  per process, as designed. A second kill of the same worker, merge or unsplit task
  is one with the retry memory: the controller fails the run and stops its workflow,
  with a message naming the retry memory. The retry limit of a pod is 100
  (`RETRY_LIMIT`): every eviction by Kueue counts against it, and a pod of a long run
  of low priority on a busy cluster may be evicted many times.
- **Stopping a workflow.** After `shutdown: Stop` Argo fails the nodes still pending
  (`workflow shutdown with strategy: Stop`); the backend deletes their gated pods.
- **Logs.** The logs of an attempt whose pod Kueue deleted are gone with the pod.
- **Priority classes.** The pods name a Kubernetes `PriorityClass`, which must exist;
  it has `preemptionPolicy: Never`, so only Kueue preempts.
- **Registry and builds.** The nodes and BuildKit name the registry as
  `ClusterConfig.registry`, the service reaches its API at
  `ClusterConfig.registry_url`. BuildKit runs rootless (seccomp and AppArmor
  unconfined, `--oci-worker-no-process-sandbox`); a build is a Job of `buildctl` whose
  context is a ConfigMap (at most 1 MB: the Dockerfile and the simrun wheel). The
  service follows the Job by reading it, not its `jobs/status` subresource, for which
  its Role has no right; a unit test checks the Role of `deploy/cluster/simrun.yaml`
  against every call of the backend, since the cluster tests run with the admin
  kubeconfig.
