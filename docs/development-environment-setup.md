# Development Environment Setup

This guide describes the process of setting up a registry for development
purposes. These instructions are not intended to represent best practices for
production environments.

## From Source

To install the container registry from source, we need to clone the source code,
make binaries and execute the `registry` binary.

### Requirements

You will need to have Go installed on your machine. You must use one of the
officially supported Go versions. Please refer to the Go [release policy](https://golang.org/doc/devel/release.html#policy)
and [install documentation](https://golang.org/doc/install) for guidance.

### Building

> These instructions assume you are using a Go version with [modules](https://golang.org/ref/mod) support enabled.

1. Clone repository:

    ```shell
    git clone git@gitlab.com:gitlab-org/container-registry.git
    cd container-registry
    ```

1. Make binaries:

    ```shell
    make binaries
    ```

    This step should complete without any errors. If not, please double check
    the official Go install documentation and make sure you have all build
    dependencies installed on your machine.

### Running

This command will start the registry in the foreground, listening at
`localhost:5000`:

```shell
./bin/registry serve config/filesystem.yml
```

The configuration file [`config/filesystem.yml`](../config/filesystem.yml) is a sample
configuration file with the minimum required settings, plus some recommended
ones for a better development experience. Please see the [configuration documentation](configuration.md) for more details and additional
settings.

## Docker

### Requirements

This guide will use Docker to simplify the deployment process. Please ensure
that you can run the `docker` command in your environment.

### Building

This command will build the registry from the code in the Git repository,
whether the changes are committed or not. After this command completes, we
will have local access to a Docker image called `registry:dev`. You may choose
to use any name:tag combination, and you may also build multiple images with
different versions of the registry for easier comparison of changes.

This command will use the `Dockerfile` in the root of this repository.

```shell
docker build -t registry:dev .
```

### Running

This command will start the registry in a Docker container, with the API
listening at `localhost:5000`.

The registry name, `dev-registry`, can be used to easily reference the container
in Docker commands and is arbitrary.

This container is ran with host networking on linux machines. This option facilitates an easier
and more general configuration, especially when using external services, such as
GCS or S3, but also removes the network isolation that a container typically
provides.

```shell
docker run -d \
    --restart=always \ 
    --network=host \
    --name dev-registry \
    -v `pwd`/config/filesystem.yml:/etc/docker/registry/config.yml \
    registry:dev
```

The host networking driver in the command above [only works on Linux hosts](https://docs.docker.com/network/host/)
and is not supported on Docker Desktop for Mac, Docker Desktop for Windows, or Docker EE
for Windows Server. The reason for this behavior is that on Mac and Windows, Docker daemon is
actually running in a virtual machine, not natively on the host. Thus, it is not actually connected
to the host ports of your Mac or Windows machine, but rather to the host ports of the virtual machine.

A way around this is to port-forward the desired container port to localhost by adding
 `-p 5000:5000` and removing the `--network=host` option in the command above, to become:

```shell
docker run -d \
    --restart=always \ 
    --name dev-registry \
    -v `pwd`/config/filesystem.yml:/etc/docker/registry/config.yml \
    -p 5000:5000 \
    registry:dev
```

The configuration file [`config/filesystem.yml`](../config/filesystem.yml) is a sample
configuration file with the minimum required settings, plus some recommended
ones for a better development experience. Please see the [configuration documentation](configuration.md) for more details and additional
settings.

### Logs

The registry logs can be accessed with the following command:

```shell
docker logs -f dev-registry
```

## Insecure Registries

For development purposes, you will likely use an unencrypted HTTP connection
(the default when using the provided sample configuration file) or self-signed
certificates.

In this case, you must instruct Docker to treat your registry as insecure.
Otherwise, you will not be able to push/pull images. Please follow the
instructions [here](https://docs.docker.com/registry/insecure/) to configure your
Docker daemon.

## Verification

If everything is running correctly, the following command should produce this
output:

```shell
curl localhost:5000/v2/_catalog
{"repositories":[]}
```

You can now try to build and push/pull images to/from your development registry,
for example:

```shell
docker pull alpine:latest
docker tag alpine:latest localhost:5000/alpine:latest
docker push localhost:5000/alpine:latest
docker pull localhost:5000/alpine:latest
```

If you are using macOS, you may need to use `0.0.0.0` instead of `localhost`.

## Metadata database setup

Follow [this document](database-local-setup.md) for local environment setup with the metadata database enabled.

## Alternatives to Docker Desktop

If you use macOS without [Docker Desktop](https://hub.docker.com/editions/community/docker-ce-desktop-mac), you must set up:

- An alternative container runtime such as [`containerd`](https://containerd.io/) or the open-source implementation of Docker ([moby](https://github.com/moby/moby)).
- A client compatible with registry v2 API to push and pull images from your local container registry.

[Colima](#colima) and [Rancher Desktop](#rancher-desktop) can be used with
[GDK](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/README.md) without much effort.

### Colima

[Colima](https://github.com/abiosoft/colima) provides compatibility with the `dockerd` (default) and `containerd` container runtimes.

1. Install the Docker CLI and Colima:

   ```shell
   brew update
   brew install docker
   brew install colima
   ```

1. Check to see that the `docker` command is available in the terminal:

   ```shell
   docker version
   => Client: Docker Engine - Community
   Version:           20.10.12
   ...
   ```

1. Start Colima:

   ```shell
   colima start
   ```

1. Test the Docker runtime is working by pulling an image from Docker Hub:

   ```shell
   docker login
   docker pull alpine:latest
   ```

To work with an insecure local registry (HTTP for push and pull),
[update the Colima configuration](https://github.com/abiosoft/colima/blob/main/docs/FAQ.md#how-to-customize-docker-config-eg-add-insecure-registries)
with any insecure local registries you are using.

1. Assuming your local registry is running, you can now log in and push images to the insecure registry:

   ```shell
   docker login
   ...
   docker push 172.16.123.1:5000/my/image:tag
   ```

### Rancher Desktop

[Rancher Desktop](https://rancherdesktop.io/) installs the `docker` daemon along with a Kubernetes cluster.

If you need to set up an insecure registry (push and pull over HTTP), you must also:

1. Shell into the `docker` virtual machine created by Rancher Desktop
(as discussed in <https://github.com/rancher-sandbox/rancher-desktop/discussions/1477>):

   ```shell
   LIMA_HOME="$HOME/Library/Application Support/rancher-desktop/lima" "/Applications/Rancher Desktop.app/Contents/Resources/resources/darwin/lima/bin/limactl" shell 0
   ```

1. In the shell, run `sudo vi /etc/docker/daemon.json`.
1. Add the following content:

   ```json
   {
      "insecure-registries" : ["172.16.123.1:5000"]
   }
   ```

1. Restart Rancher Desktop (quit and open it again):
1. You should be able to push to your local insecure registry:

   ```shell
   docker login 172.16.123.1:5000 -u $GITLAB_USER -p $GITLAB_PRIVATE_TOKEN
   docker push 172.16.123.1:5000/my-alpine:latest
   The push refers to repository [172.16.123.1:5000/my-alpine]
   8d3ac3489996: Layer already exists
   latest: digest: sha256:e7d88de73db3d3fd9b2d63aa7f447a10fd0220b7cbf39803c803f2af9ba256b3 size: 528
   ```

### lima+nerdctl

Lima launches Linux virtual machines with automatic file sharing and port forwarding (similar to WSL2), and `containerd`.

A [Lima](https://github.com/lima-vm/lima) virtual machine can be easily installed on macOS
that will run `containerd` as the container runtime. You can also use
[nerdctl](https://github.com/containerd/nerdctl) as a substitute for Docker CLI.
[Ash McKenzie](https://gitlab.com/ashmckenzie) created a [snippet](https://gitlab.com/-/snippets/2203856) with a one-line installation step.

If you prefer to set them up manually:

1. Install dependencies (`qemu` and `lima`) and create a directory (optional):

   ```shell
   brew update
   brew install qemu lima
   mkdir -p ~/lima/docker/
   cd ~/lima/docker/
   ```

1. In the `docker` directory that you just created, create a Docker virtual machine configuration file and name it `docker.yml`. For example:

   ```yaml
   # Original content https://gitlab.com/-/snippets/2203856/raw/main/docker.yaml
   arch: "default"

   images:
     - location: "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-arm64.img"
       arch: "aarch64"
     - location: "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img"
       arch: "x86_64"
   cpus: 2
   memory: "4GiB"
   disk: "30GiB"
   mounts:
     - location: "~"
       # CAUTION: `writable` SHOULD be false for the home directory.
       # Setting `writable` to true is possible, but untested and dangerous.
       writable: false
   ssh:
     localPort: 60022
     loadDotSSHPubKeys: false

   containerd:
     system: false
     user: true

   firmware:
     legacyBIOS: false

   video:
     display: "none"
   ```

1. Create the virtual machine using `limactl`:

   ```shell
   limactl start --tty=false docker.yml
   ```

1. Docker will run inside your virtual machine with `nerdctl` available to use.
   You need to shell into the virtual machine to be able to push and pull images, as well as run containers. To test, run the
   following commands:

   ```shell
   # pull an image from DockerHub
   $ limactl shell docker nerdctl pull alpine:latest
   $ limactl shell docker nerdctl images | grep alpine
   alpine                                                                latest     21a3deaa0d32    13 days ago     linux/amd64    5.9 MiB
   ```

1. Optionally, you can set up an alias for the same command:

   ```shell
   alias docker='limactl shell docker nerdctl'
   ```

1. Now you can run a container:

   ```shell
   $ docker run -ti --rm alpine echo "hello from alpine\!"
   hello from alpine!
   ```

1. To push to a local **insecure** container registry:

   ```shell
   docker tag alpine 172.16.123.1:5000/my-alpine:latest
   docker push 172.16.123.1:5000/my-alpine:latest
   ```

Because `containerd` is running inside the virtual machine, you must
use your host's IP address such as `172.16.123.1` or hostname `registry.test`
configured on a [loopback interface](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/doc/howto/local_network.md#local-network-binding).

## Troubleshooting

### bind: cannot assign requested address on macOS

If you are using macOS 12+ Monterey, you might run into a similar issue shown below when starting the registry

```shell
2022-02-17_05:16:32.77124 registry              : docker: Error response from daemon: driver failed programming external connectivity on endpoint objective_blackburn (e5b9e04c3f7990dcf143eb0b3b35c83f66ef8ea52f3a47ea92bd0b8765151624): 
Error starting userland proxy: listen tcp4 172.16.123.1:5000: bind: cannot assign requested address.
```

It is possible that another service is listening on port `5000` such as `AirPlay Receiver` as described on this
[Apple developer forum post](https://developer.apple.com/forums/thread/682332).

You may check if the port is in use, using: `lsof -i tcp:5000`.

If the culprit happens to be `AirPlay Receiver`, you can disable it by opening **System Preferences > Sharing** and unchecking **AirPlay Receiver**.

### $XDG_RUNTIME_DIR is not set

If you are running `lima+nerdctl` and get an error about `$XDG_RUNTIME_DIR` not being set, ensure there are no other
`nerdctl` installations such as [Rancher Desktop](#rancher-desktop) and restart the Docker virtual machine.
