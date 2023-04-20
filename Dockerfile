# Use Python 3.8.16-bullseye as base image
ARG TAG=latest
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:${TAG}

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="pipeline.py"

COPY ./requirements.txt /requirements.txt
COPY ./pipeline.py /pipeline.py

# We could get rid of installing libffi-dev and git, or we could leave them.
RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    # Upgrade pip and install the requirements.
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    # Download the requirements to speed up launching the Dataflow job.
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True

RUN python setup.py install

# Set the command to run the Dash app
#CMD ["python", "pipeline.py"]
