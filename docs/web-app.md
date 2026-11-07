# Web app

The service serves a web app at `/ui/`, e.g. `http://localhost:8000/ui/` on the [local test system](testing.md#a-local-test-system). It shows the runs of people and agents live: the **Executions**. The app talks only to the API of the service, with the token of the user; the service serves it from the directory of its setting `web` (`/opt/simrun/web` in the [service image](deployment.md#the-service-image)), and without that setting there is no `/ui/`.

## Login

The app asks for a token of the tokens file of the service (see [Authentication](service.md#authentication)), checks it with `GET /me` and keeps it in the local storage of the browser. Every request and every event stream sends it as a bearer token, never in a URL. A token the service no longer accepts (removed from the tokens file) returns the app to the login and removes the token from the browser.

## Executions

The view has the runs on the left, the canvas of the selected run in the center with the run bar above it and the servers below it, and the inspector on the right. Everything is live: the app reads the event streams `GET /events` (the runs and the servers) and `GET /runs/{run}/events` (the record, the progress and the workers of the selected run). When a stream drops, e.g. while the service restarts, the top bar shows **reconnecting** and the app connects again, waiting 1 s, doubling up to 10 s, and keeps the selected run.

- **Top bar**: where you are (the workflow of the selected run), the capacity of the cluster (the cores the pods of all servers request out of all cores, and the number of queued runs) and the identity of the token.
- **Runs**: a card per run, the 200 most recent, with the workflow, the run, who submitted it (an agent with its owner), its state, its progress and, for a failed run, its message. The chips **All**, **Mine** (the runs of the person of the token and of the person's agents), **Agents** and **Active** (submitting, queued or running) filter them, a menu selects a group. A click selects the run; the address `/ui/#/runs/<run>` keeps it on a reload.
- **Run bar**: the run, who submitted it and when, its state and its progress (tasks done, units done of all units), and **Stop**, which cancels a run that is not over after a confirmation in the bar.
- **Canvas**: a card per task, in columns by the depth of the task in the graph of its inputs, with the edges of the inputs. A card shows the type, the task, its state, its settings, the units of its ledger with a progress bar and the workers of a split task on their servers; a task split over several workers is drawn as a stack. A task without a ledger shows its state without a progress bar.
- **Servers**: a tile per server of the cluster with the cores and memory its pods request, a segment per pod, those of the selected run in blue; a reserved server says by which run.
- **Inspector**: without a selected task, the run: its message, who submitted it with which priority and when, its kills for memory and its sources (the snapshots with branch, commit and changed files). With a task selected on the canvas, the tabs **Overview** (the units, the elapsed and the remaining time, the numeric fields of the last progress line, the workers with their servers, processes and memory, and the resources its plan requested), **Outputs** (the output files of the task) and **Logs** (the last lines of every pod, read again every 5 s while the task runs).

## Development

The app is a Vite project in `frontend/` (Vue 3, TypeScript, Vue Flow, Tailwind CSS) with node 24 and the versions of `frontend/package-lock.json`:

```bash
cd frontend
npm ci                 # the dependencies of the lock
npm run dev            # the dev server at http://localhost:5173/ui/, with reload on every change
npm test               # the tests (Vitest, jsdom)
npm run typecheck      # vue-tsc
npm run build          # into frontend/dist, which the service serves with `web: frontend/dist`
```

The dev server proxies the API to the service of `SIMRUN_URL`, else `http://localhost:8000`, so it runs against the service of the [local test system](testing.md#a-local-test-system): start the service as described there, submit a run, and open `http://localhost:5173/ui/` with the token `t-mk`. The service of the local test system also serves the built app at `http://localhost:8000/ui/` once `npm run build` made `frontend/dist`.

The types of the API in `frontend/src/api/schema.d.ts` are generated from the OpenAPI document of the service. After a change of the API:

```bash
uv run python scripts/openapi.py   # frontend/src/api/openapi.json, which a test keeps equal to the API
cd frontend && npm run types       # frontend/src/api/schema.d.ts
```

`frontend/mockup/` is the approved mockup the app is built from, a single page with data in memory.
