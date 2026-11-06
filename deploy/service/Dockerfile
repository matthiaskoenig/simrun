# syntax=docker/dockerfile:1
# The image of the service (`deploy/cluster/simrun.yaml`): uv, which resolves the
# environments of the runs, simrun with the dependencies of its lock, its wheel at
# the path of `simrun_wheel` of the settings (the images of the environments install
# it), and the plugins whose task types the service validates and splits.
#
# The build context, see "The service image" in the README:
#   requirements.txt  `uv export --frozen --no-dev --no-emit-project` of simrun
#   simrun-<version>-py3-none-any.whl
#   plugins/          the wheels of the plugins (may be empty)
ARG BASE=registry.cluster.internal:5000/astral-sh/uv:0.13.7-python3.14-trixie-slim@sha256:e80c05a310400cfc90af00b5b7271ef9ad24816e3265e1162af8a678e1aad099
FROM ${BASE}
ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"
RUN uv venv /opt/venv --python 3.14
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --no-deps --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
ARG WHEEL=simrun-0.1.0-py3-none-any.whl
COPY ${WHEEL} /opt/simrun/${WHEEL}
RUN uv pip install --no-deps /opt/simrun/${WHEEL}
COPY plugins/ /tmp/plugins/
RUN if ls /tmp/plugins/*.whl > /dev/null 2>&1; then \
        uv pip install /tmp/plugins/*.whl; \
    fi \
    && rm -rf /tmp/plugins
EXPOSE 8000
CMD ["uvicorn", "--factory", "simrun.service.app:serve", "--host", "0.0.0.0", "--port", "8000"]
