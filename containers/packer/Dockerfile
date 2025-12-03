FROM debian:trixie

VOLUME ["/root/.config"]
SHELL ["/bin/bash", "-c"]

RUN apt update && \
    apt install -q -y \
        apt-transport-https \
        ca-certificates \
        lsb-release \
        curl \
        idn2 \
        unzip \
        gnupg \
        git \
        python3-pip \
        python3-full \
        pipx \
        jq

RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor > /usr/share/keyrings/google.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" >> /etc/apt/sources.list.d/tooling.list && \
    apt update && \
    apt install -q -y google-cloud-cli && \
    apt-get clean

RUN pipx install --include-deps ansible && \
    pipx inject ansible google-auth netaddr requests && \
    pipx ensurepath

# Install packer
RUN curl -L https://releases.hashicorp.com/packer/1.14.2/packer_1.14.2_linux_amd64.zip -o "packer_linux_amd64.zip" && \
    unzip packer_linux_amd64.zip packer && \
    chmod +x packer && \
    mv packer /usr/local/bin/packer && \
    packer --version

RUN gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image
