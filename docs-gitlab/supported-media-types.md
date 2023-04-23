# Supported Media Types

The following is the list of supported media types. Any manifest or any of its references with a media type outside this list will lead to a `400 Bad Request` response with detail `unknown media type` when trying to push it to the registry.

| Media Type                                                               |
| ------------------------------------------------------------------------ |
| `text/spdx`                                                              |
| `text/plain; charset=utf-8`                                              |
| `text/html; charset=utf-8`                                               |
| `binary/octet-stream`                                                    |
| `appliciation/vnd.sylabs.sif.layer.tar`                                  |
| `application/x-yaml`                                                     |
| `application/vnd.wasm.content.layer.v1+wasm`                             |
| `application/vnd.wasm.config.v1+json`                                    |
| `application/vnd.vivsoft.enbuild.config.v1+json`                         |
| `application/vnd.unknown.config.v1+json`                                 |
| `application/vnd.sylabs.sif.layer.v1.sif`                                |
| `application/vnd.sylabs.sif.config.v1+json`                              |
| `application/vnd.sylabs.sif.config.v1`                                   |
| `application/vnd.spack.package`                                          |
| `application/vnd.oras.config.v1+json`                                    |
| `application/vnd.oci.image.manifest.v1+json`                             |
| `application/vnd.oci.image.layer.v1.tar+zstd`                            |
| `application/vnd.oci.image.layer.v1.tar+gzip+encrypted`                  |
| `application/vnd.oci.image.layer.v1.tar+gzip`                            |
| `application/vnd.oci.image.layer.v1.tar+encrypted`                       |
| `application/vnd.oci.image.layer.v1.tar`                                 |
| `application/vnd.oci.image.layer.nondistributable.v1.tar+gzip`           |
| `application/vnd.oci.image.layer.nondistributable.v1.tar`                |
| `application/vnd.oci.image.index.v1+json`                                |
| `application/vnd.oci.image.config.v1+json`                               |
| `application/vnd.module.wasm.content.layer.v1+wasm`                      |
| `application/vnd.module.wasm.config.v1+json`                             |
| `application/vnd.gitlab.packages.npm.config.v2+json`                     |
| `application/vnd.gardener.landscaper.componentdefinition.v1+json`        |
| `application/vnd.gardener.landscaper.blueprint.v1+tar+gzip`              |
| `application/vnd.gardener.cloud.cnudie.component.config.v1+json`         |
| `application/vnd.gardener.cloud.cnudie.component-descriptor.v2+yaml+tar` |
| `application/vnd.gardener.cloud.cnudie.component-descriptor.v2+json`     |
| `application/vnd.dsse.envelope.v1+json`                                  |
| `application/vnd.docker.plugin.v1+json`                                  |
| `application/vnd.docker.image.rootfs.foreign.diff.tar.gzip`              |
| `application/vnd.docker.image.rootfs.diff.tar`                           |
| `application/vnd.docker.image.rootfs.diff.tar.gzip`                      |
| `application/vnd.docker.distribution.manifest.v2+json`                   |
| `application/vnd.docker.distribution.manifest.v1+prettyjws`              |
| `application/vnd.docker.distribution.manifest.v1+json`                   |
| `application/vnd.docker.distribution.manifest.list.v2+json`              |
| `application/vnd.docker.container.image.v1+json`                         |
| `application/vnd.docker.container.image.rootfs.diff+x-gtar`              |
| `application/vnd.dev.cosign.simplesigning.v1+json`                       |
| `application/vnd.cncf.openpolicyagent.policy.layer.v1+rego`              |
| `application/vnd.cncf.openpolicyagent.manifest.layer.v1+json`            |
| `application/vnd.cncf.openpolicyagent.data.layer.v1+json`                |
| `application/vnd.cncf.openpolicyagent.config.v1+json`                    |
| `application/vnd.cncf.helm.config.v1+json`                               |
| `application/vnd.cncf.helm.chart.provenance.v1.prov`                     |
| `application/vnd.cncf.helm.chart.meta.layer.v1+json`                     |
| `application/vnd.cncf.helm.chart.content.v1.tar+gzip`                    |
| `application/vnd.cncf.helm.chart.content.layer.v1+tar`                   |
| `application/vnd.cncf.artifacthub.repository-metadata.layer.v1.yaml`     |
| `application/vnd.cncf.artifacthub.config.v1+yaml`                        |
| `application/vnd.cnab.config.v1+json`                                    |
| `application/vnd.cnab.bundle.config.v1+json`                             |
| `application/vnd.buildkit.cacheconfig.v0`                                |
| `application/vnd.aquasec.trivy.db.layer.v1.tar+gzip`                     |
| `application/vnd.aquasec.trivy.config.v1+json`                           |
| `application/vnd.ansible.collection`                                     |
| `application/vnd.acme.rocket.docs.layer.v1+tar`                          |
| `application/vnd.acme.rocket.config`                                     |
| `application/vnd.acme.rocket.config`                                     |
| `application/tar+gzip`                                                   |
| `application/sap-cnudie+tar`                                             |
| `application/octet-stream`                                               |
| `application/json`                                                       |

The list above should be updated by engineers whenever modifying the `media_types` database table, keeping entries in alphabetical descending order.
