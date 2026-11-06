# Development

Contributions are welcome. The repository is [matthiaskoenig/simrun](https://github.com/matthiaskoenig/simrun); development happens against the `develop` branch via pull requests.

## Branch model

Two branches are permanent:

- **`develop`** is the default branch and the branch everything is integrated into. The documentation on [matthiaskoenig.github.io/simrun](https://matthiaskoenig.github.io/simrun) is published from it.
- **`main`** tracks the latest published release. It is fast-forwarded to the released commit by the `sync-main` job of the `CI-CD` workflow after the package went to PyPI, so `main` and the newest version on PyPI always agree. Nothing is developed on `main` and nothing is merged into it by hand.

Work happens on short lived branches off `develop`, which GitHub deletes after the merge. Releases are tagged on `develop`, see [Release](#release).

## Pull requests

Neither branch accepts a direct push, every change goes through a pull request against `develop`. This includes the maintainer, there is no bypass.

A pull request can only be merged once the four required checks are green:

| check | workflow | content |
| --- | --- | --- |
| `tests` | `ci-cd.yml` | the unit tests on python 3.14 and 3.15 on Linux and the plain install |
| `ruff` | `lint.yml` | `ruff check` and `ruff format --check`, with the ruff of `uv.lock` |
| `ty` | `lint.yml` | `ty check` |
| `docs` | `docs.yml` | the strict zensical build including the API reference and the agent files |

`tests` aggregates the test matrix and the job `plain install` into a single job, so the name of the required check stays the same when the matrix changes. `plain install` installs the package without extras, as `pip install simrun` does, imports every module and runs `simrun --help`: the test matrix has the extras of `dev`, which would hide a dependency the package needs and does not declare. It is the one job which does not use the lock, see [Dependencies](#dependencies).

The tests on the k3d cluster, the job `cluster` (`cluster.yml`), take about half an hour and are no required check. They run on every push to `develop`, on a release, which waits for them, and when `CI-CD` is started by hand (`workflow_dispatch`). A pull request which changes what runs on the cluster gets the label `cluster`: the tests run once the label is added and with every push after it.

Every job sets up its environment with the action `.github/actions/setup`: uv with its cache of the packages and the interpreters, the python of the job and `uv sync --locked` with the extras of the job, i.e. the versions of `uv.lock`. The key of the cache of uv has the os and the python version but not the job, so jobs on the same python would race to save it and warn `Unable to reserve cache`: only the jobs of the test matrix save the cache (`save-cache: "true"`), the others restore it. The jobs run the same commands as a local environment (`pytest`, `ty check`, `zensical build`), the tests against the package installed as a wheel (`uv sync --no-editable`). A run of a pull request is cancelled by the next push to it; a push to `develop` or `main` is never cancelled, a release waits for the run of its commit. The release builds the distributions without the cache (`cache: "false"`). Dependabot proposes its updates once a month.

Further rules of a pull request:

- conversations have to be resolved before the merge
- an approval is dismissed when new commits are pushed
- the history stays linear, i.e., a pull request is merged with squash or rebase; merge commits are disabled
- the maintainer is the code owner of the repository (`.github/CODEOWNERS`) and is requested for review on every pull request. The rulesets themselves do not require an approval: on a personal repository a ruleset cannot ask for an approval only from somebody else, and requiring one would block the pull requests of the maintainer, who cannot approve their own

[Auto-merge](https://docs.github.com/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/automatically-merging-a-pull-request) is enabled for the repository, so a pull request can be queued and is merged as soon as the checks pass.

### Repository policies { #repository-policies }

The protection is implemented with [repository rulesets](https://docs.github.com/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets). They are part of the repository in `.github/rulesets/` instead of only living in the web interface, so a change to a policy is reviewed like any other change:

| ruleset | applies to | rules |
| --- | --- | --- |
| `develop.json` | `develop` | pull request required, the four checks above, resolved conversations, linear history, no force push, no deletion. **No bypass, for anybody.** |
| `main.json` | `main` | linear history, no force push, no deletion, no bypass. The fast-forward of the release workflow needs none, only a force push would be rejected |
| `tags.json` | all tags | a tag cannot be deleted or moved, so a release tag keeps pointing at what was released |

Changing a policy means changing the json and applying it:

```bash
.github/rulesets/apply.sh
```

The script is idempotent: it updates the rulesets which exist and creates the missing ones. It also sets the merge settings of the repository, i.e., auto-merge, delete branch on merge, and squash and rebase as the only merge methods. It needs the [github cli](https://cli.github.com) authenticated as a user with admin permission on the repository.

## Setup development environment

Development needs [uv](https://docs.astral.sh/uv/) and a checkout of the repository:

```bash
git clone https://github.com/matthiaskoenig/simrun.git
cd simrun
```

A single sync creates the virtual environment in `.venv`, installs `simrun` into it in editable mode and adds the complete tooling, in the versions of `uv.lock`:

```bash
uv sync --extra dev
```

The `dev` extra contains everything used below, i.e., pytest, ruff, ty, tox with tox-uv, pre-commit, zensical, bump-my-version and the extra `cwl`, so nothing has to be installed separately. The python version is taken from `.python-version` (currently 3.14). The tools are then run either with `uv run <command>`, which uses the environment without activating it, or from the activated environment (`source .venv/bin/activate`). The commands in this document are written without the `uv run` prefix; prepend it if the environment is not activated.

The last step installs the git hook:

```bash
uv run pre-commit install          # install the hook, once per checkout
uv run pre-commit run --all-files  # check the current state of the repository
```

From now on every commit is checked with ruff (lint and format) and ty, i.e., the same checks that run in continuous integration. On a commit only the changed files are looked at, `--all-files` checks the whole repository and is what a newly added hook should be tried with. The release manifests of Argo Workflows and Kueue in `deploy/cluster` are committed as released and excluded from the hooks.

## Dependencies

The environment is resolved from `uv.lock`, which is committed. It pins every package of the environment, i.e. the dependencies, the extra `cwl` and the tools of `dev`, so a local `uv sync` and continuous integration install the same versions and a result can be reproduced; the image of the service installs simrun with the dependencies of the same lock. Every job which syncs through the action `.github/actions/setup` runs against the lock, and the `ruff` check installs the ruff version of the lock (`version-file: uv.lock`), so continuous integration and `uv run ruff` agree.

Three things do not use the lock on purpose. The job `plain install` resolves from `pyproject.toml` as `pip install simrun` does, i.e. the newest releases within the bounds, so a release of a dependency which breaks simrun shows up there before the lock is moved to it. The tox environments resolve from `pyproject.toml` as well. The release builds and checks the package with `uvx hatch` and `uvx twine`. The lower bounds in `pyproject.toml` are what a user of the library installs against, the lock only records the versions the development runs with.

After changing a dependency in `pyproject.toml` run `uv lock`: the action syncs with `uv sync --locked`, which fails when `pyproject.toml` and the lock disagree, so the change and the `uv lock` which records it go into the same pull request. `uv lock --upgrade` moves every package of the lock to its newest release, `uv lock --upgrade-package <name>` a single one.

Dependabot keeps the lock current: once a month it opens one pull request which bumps the packages of the lock together, with the prefix `build`, and one which bumps the GitHub Actions, with the prefix `ci`. Its `versioning-strategy: lockfile-only` only changes `uv.lock`, the lower bounds in `pyproject.toml` are never raised by it and change by hand. The revisions of the ruff and ty hooks in `.pre-commit-config.yaml` are the versions of the lock, which dependabot does not update: when such a pull request moves ruff or ty, raise the revision of the hook to the same version.

## Testing

The tests are written with pytest, tox runs them against every supported python version (`py3.14`, `py3.15`, see `envlist` in `tox.ini`):

```bash
tox r -e py3.14       # a single environment
tox run-parallel      # every supported python, before a pull request is opened
```

This needs the interpreters, which uv installs with `uv python install 3.14 3.15`. To run the tests directly against the development environment use `pytest`; the tests run in parallel, `-n auto` in the `addopts` of `pyproject.toml` gives pytest-xdist one worker per core, `-n 0` runs everything in one process, which the debugger needs. The tests on the k3d cluster are described in [Testing](testing.md#tests-on-a-k3d-cluster).

## Linting and formatting

Linting and formatting use [ruff](https://docs.astral.sh/ruff/):

```bash
ruff check     # lint
ruff format    # format
```

The rules are in `.ruff.toml`; among them the docstring rules (`D`, google convention), which are enforced for the package and not for `tests/`, and the rules of logging calls (`G`): library code logs with lazy `%s` formatting rather than f-strings.

## Type checking

Type checking is performed with [ty](https://docs.astral.sh/ty/):

```bash
uv run ty check
```

ty resolves the imports in the environment of the project, which `uv sync --extra dev` creates; the `ty` check of continuous integration runs the same command and the pre-commit hook runs it on every commit. The configuration lives in `[tool.ty]` in `pyproject.toml`. Warnings are treated as errors, so the codebase is kept free of diagnostics. Suppress an unavoidable diagnostic with a rule specific `# ty: ignore[rule-name]` rather than a blanket comment.

## Documentation

The documentation is built with [Zensical](https://zensical.org/), the static site generator of the Material for MkDocs authors. The sources are markdown files in `docs/`, the site is configured in `zensical.toml` in the repository root. Nothing rendered is committed: the site is built by the `documentation` workflow on every push and published to [matthiaskoenig.github.io/simrun](https://matthiaskoenig.github.io/simrun) from the `develop` branch.

Build the site into `site/`, strict as continuous integration builds it, i.e. a warning fails the build:

```bash
uv run zensical build --clean --strict
```

For writing, the preview rebuilds on save:

```bash
uv run zensical serve
```

The API reference is rendered from the docstrings by [mkdocstrings](https://mkdocstrings.github.io/); a page in `docs/api/` only contains the module directive:

```markdown
# ledger

::: simrun.ledger
```

Docstrings are therefore the place to document functions and classes, the markdown files provide the narrative around them. Adding a module to the reference means adding such a page, an entry to `nav` in `zensical.toml` and a row of `docs/api/index.md`. Markdown carries no hard line wraps: a paragraph, a list item or a table row is a single line.

### Files for agents { #files-for-agents }

Agents and language models read markdown, not rendered html. `scripts/llms_txt.py` writes the files of the [llms.txt convention](https://llmstxt.org/) into the built site, i.e., [llms.txt](https://matthiaskoenig.github.io/simrun/llms.txt) as an annotated index of all pages, [llms-full.txt](https://matthiaskoenig.github.io/simrun/llms-full.txt) with the complete documentation in a single file, and the markdown of every page next to its html. The markdown of the API reference is generated from the docstrings with `inspect`, since the pages themselves only contain the mkdocstrings directive.

```bash
uv run zensical build --clean --strict
uv run python scripts/llms_txt.py
```

The `documentation` workflow runs both steps, so the files are regenerated with every push. `docs/robots.txt` points crawlers at the sitemap and at these files.

## Release

A release is made from `develop`. Since `develop` only accepts pull requests, the release is prepared on a branch and tagged once that pull request is merged:

1. branch off `develop`: `git switch -c release/x.y.z develop`
2. write the release notes for the version in `release-notes/x.y.z.md` and add them to `docs/release-notes.md`
3. make sure everything passes: `tox run-parallel`, `ruff check`, `uv run ty check`
4. check the version bump: `uvx bump-my-version bump [major|minor|patch] --dry-run -vv`
5. bump the version: `uvx bump-my-version bump [major|minor|patch]`, which updates `src/simrun/__init__.py` and `CITATION.cff` and commits. It does not create the tag; a squash or rebase merge would rewrite the commit and leave the tag behind on a commit which is not part of `develop`
6. push the branch, open the pull request against `develop` and merge it once the checks are green
7. tag the merged commit on `develop` and push the tag:

    ```bash
    git switch develop
    git pull
    git tag x.y.z
    git push origin x.y.z
    ```

    This starts the `CI-CD` workflow, which checks that the tag is the version of the package, builds the sdist and the wheel, publishes them to [PyPI](https://pypi.org/project/simrun/), creates the GitHub release from `release-notes/x.y.z.md` and fast-forwards `main` to the tagged commit. It does not run the test matrix again: the merge into `develop` ran it on the same commit, and the job `tested on develop` waits for that run and stops the release unless it passed, so the tag can be pushed right after the merge. Next to it the tests on the k3d cluster run on the tag, and the release waits for them. Check the version before pushing, a tag cannot be moved or deleted afterwards.

8. test the installation from PyPI in a fresh environment:

    ```bash
    uv venv --python 3.14
    uv pip install simrun
    ```

9. once Zenodo has archived the release, update the citation information, i.e., `date-released` and `doi` in `CITATION.cff` and the DOI badge and the citation of `README.md` and `docs/index.md`. `bump-my-version` only updates the version, not the date and the DOI, which are only known after the release. These changes go in through a pull request like everything else

### PyPI and Zenodo

The package is published with [trusted publishing](https://docs.pypi.org/trusted-publishers/): the job `release` runs in the GitHub environment `pypi` with the permission `id-token: write`, and PyPI accepts the distributions of this workflow without a token. The publisher is registered on PyPI for the project `simrun`, the owner `matthiaskoenig`, the repository `simrun`, the workflow `ci-cd.yml` and the environment `pypi`.

[Zenodo](https://zenodo.org) archives every GitHub release of the repository with the metadata of `.zenodo.json` and gives it a DOI; the first release creates the concept DOI, which stands for every version and goes into the badge and the citation.
