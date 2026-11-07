# Web app

The web app of simrun: Vue 3, Vue Flow and Tailwind, served by the service and talking only to its API. Its three views:

- **Editor**: the task types of the installed plugins as a palette, a canvas of the tasks and their inputs, the settings of a task as a form of its JSON Schema, its resources (learned memory, the estimate of duration and placement, split mode, priority, reservation), the sources, save and run.
- **Executions**: the runs of people and agents with filters and groups, the canvas of a run with the live state of every task, progress and metrics from `progress.jsonl`, the pods of a split task with their servers, the outputs and the logs, a strip of the servers with the cores and memory used per run; live updates by server-sent events.
- **Compare**: the runs of a group side by side.

The service serves the build (`dist/`) at `/ui/` when its setting `web` names it; the service image has it at `/opt/simrun/web`. Node 24, the versions of `package-lock.json`:

```bash
npm ci                 # the dependencies of the lock
npm run dev            # the dev server at http://localhost:5173/ui/, the API proxied to SIMRUN_URL or http://localhost:8000
npm test               # the tests (Vitest, jsdom)
npm run typecheck      # vue-tsc of the app and of the configuration
npm run build          # the app into dist/
npm run types          # src/api/schema.d.ts from src/api/openapi.json, after `uv run python scripts/openapi.py` in the root
```

The dev server runs against the service of the local test system (`docs/testing.md`, "A local test system"); `docs/web-app.md` describes the views and the development.

`mockup/` is the approved mockup, a single page with data in memory: open `mockup/workflow-studio.html` in a browser. The web app itself is built from it, the Executions first, then the Editor.
