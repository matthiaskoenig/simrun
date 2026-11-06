# Web app

The web app of simrun: Vue 3, Vue Flow and Tailwind, served by the service and talking only to its API. Its three views:

- **Editor**: the task types of the installed plugins as a palette, a canvas of the tasks and their inputs, the settings of a task as a form of its JSON Schema, its resources (learned memory, the estimate of duration and placement, split mode, priority, reservation), the sources, save and run.
- **Executions**: the runs of people and agents with filters and groups, the canvas of a run with the live state of every task, progress and metrics from `progress.jsonl`, the pods of a split task with their servers, the outputs and the logs, a strip of the servers with the cores and memory used per run; live updates by server-sent events.
- **Compare**: the runs of a group side by side.

`mockup/` is the approved mockup, a single page with data in memory: open `mockup/workflow-studio.html` in a browser. The web app itself is built from it, the Executions first, then the Editor.
