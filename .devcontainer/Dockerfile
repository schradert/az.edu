# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.194.3/containers/python-3/.devcontainer/base.Dockerfile

ARG VARIANT="3.9"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ARG NODE_VERSION="none"
RUN if [ "${NODE_VERSION}" != "none" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

RUN /usr/local/bin/python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip --no-cache-dir install -r requirements.txt
