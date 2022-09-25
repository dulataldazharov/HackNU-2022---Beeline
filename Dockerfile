ARG OWNER=jupyter
ARG BASE_CONTAINER=$OWNER/pyspark-notebook
FROM $BASE_CONTAINER

USER root

RUN mamba install --quiet --yes \
    'geospark' && \
    mamba clean --all -f -y && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"


WORKDIR "${HOME}"
RUN mkdir -p ./data/
RUN mkdir -p ./src/
COPY data/ ./data/
COPY src/ ./src/