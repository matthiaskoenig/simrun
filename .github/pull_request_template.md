## Summary

<!-- what does this change and why -->

## Checklist

- [ ] the pull request targets `develop`
- [ ] tests were added or updated for the change
- [ ] `pytest` and `ty check` pass locally, `tox run-parallel` runs the tests on every supported python
- [ ] the change to the cluster was tested on k3d (`SIMRUN_K3D=1 pytest -m cluster`) or the pull request has the label `cluster`
- [ ] `ruff check` and `ruff format` are clean, e.g., via `pre-commit run --all-files`
- [ ] public functions and classes have type annotations and a docstring
- [ ] user visible changes are in `release-notes/` and, if needed, in `docs/`
