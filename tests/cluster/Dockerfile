# The image of the runs of the cluster tests: simrun (the worker and the demo plugin)
# from the wheel of the working tree, with the dependencies of the worker only.
FROM python:3.14-slim-bookworm
RUN pip install --no-cache-dir --root-user-action=ignore "pydantic>=2.14" "pyyaml>=6.0.3"
ARG WHEEL
COPY ${WHEEL} /tmp/
RUN pip install --no-cache-dir --root-user-action=ignore --no-deps /tmp/${WHEEL} \
    && rm /tmp/${WHEEL}
# the user of the host who owns the store of the tests
ARG UID=1000
ARG GID=1000
USER ${UID}:${GID}
