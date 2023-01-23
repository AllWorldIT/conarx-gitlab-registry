## [3.65.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.65.0-gitlab...v3.65.1-gitlab) (2023-01-20)


### Bug Fixes

* **api/gitlab/v1:** repositories list endpoint should return an empty response... ([b307d2c](https://gitlab.com/gitlab-org/container-registry/commit/b307d2c48f70e5858a2f67eaa8581fb092730878))
* disable statement timeout for layers simplified usage migrations ([499e323](https://gitlab.com/gitlab-org/container-registry/commit/499e3233b4c2899741d6b0b0fa9818e2554e864a))

# [3.65.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.64.0-gitlab...v3.65.0-gitlab) (2023-01-12)


### Features

* **api/gitlab/v1:** add route for paginated list of repos under path ([c106367](https://gitlab.com/gitlab-org/container-registry/commit/c106367fd131a079978cf1787ccbd2e8f461a21a))
* deprecate azure legacy prefix ([9823a15](https://gitlab.com/gitlab-org/container-registry/commit/9823a15c95d9f0a0707577d088b3da4bac7b616d))
* deprecate proxy pull-through cache mode ([82046e1](https://gitlab.com/gitlab-org/container-registry/commit/82046e16868866732b9b35d55106b0dd19413c14))

# [3.64.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.63.0-gitlab...v3.64.0-gitlab) (2023-01-06)


### Bug Fixes

* **gc:** retry aws serialization error if wrapped as aws request failure ([4135702](https://gitlab.com/gitlab-org/container-registry/commit/41357023ce7959d8536bb546517e45375b7fe277))
* **handlers:** support http.prefix for gitlab v1 routes ([31b405c](https://gitlab.com/gitlab-org/container-registry/commit/31b405c88932b6513022a064e0c7c3526542a0ef))


### Features

* **api/gitlab/v1:** fallback to size estimate if failed to measure top-level namespaces ([74ff7ef](https://gitlab.com/gitlab-org/container-registry/commit/74ff7eff018ed7ee6c08da243d803d20ed6830e9))
* **datastore:** add remaining indexes on layers for alternative namespace usage query ([42fe2bd](https://gitlab.com/gitlab-org/container-registry/commit/42fe2bd3fabb4db46d2b4a924eb27fe512f95450))
* **datastore:** estimated namespace size with timeout awareness ([c27e2ba](https://gitlab.com/gitlab-org/container-registry/commit/c27e2bac6186749e1b45c0a4dd5fa10ff33162f8))
* **db:** introduce query for repositories (with at least 1 tag) under a path ([c37bcad](https://gitlab.com/gitlab-org/container-registry/commit/c37bcad41dac351ce76c3b3d70234f32378ca7f8))
* **handlers:** log router info for all http requests ([93d60bf](https://gitlab.com/gitlab-org/container-registry/commit/93d60bf46ff78e1d8d603787474783d49dab8be6))
* show elapsed time when applying up/down schema migrations with CLI ([4dc495d](https://gitlab.com/gitlab-org/container-registry/commit/4dc495d4ba23f376723c9503013691e0503b8b80))


### Performance Improvements

* **urls:** do not instantiate routers each time NewBuilder is called ([ebc07fb](https://gitlab.com/gitlab-org/container-registry/commit/ebc07fba999fdd87629f8eb8533a0a0b67258166))

# [3.63.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.62.0-gitlab...v3.63.0-gitlab) (2022-12-13)


### Features

* add index on layers for alternative namespace usage query (batch 1/3) ([9e8a30a](https://gitlab.com/gitlab-org/container-registry/commit/9e8a30ac6afbf94fe754929dedd52b0c3681e99b))

# [3.62.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.61.0-gitlab...v3.62.0-gitlab) (2022-12-05)


### Bug Fixes

* **handlers:** remove redundant database warn logs ([e3ae84a](https://gitlab.com/gitlab-org/container-registry/commit/e3ae84a105b65eb7de64a152c23ba8a11787f11a))


### Features

* **cache:** cache repository self size ([89e8400](https://gitlab.com/gitlab-org/container-registry/commit/89e84001395f8ece94f4c8d2f7db51d8fb58c9d7))
* **reference:** remove support for deprecated "shortid" refs ([b87363c](https://gitlab.com/gitlab-org/container-registry/commit/b87363c4b4e48ca2b78cf8770295286f793bb364))

# [3.61.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.60.2-gitlab...v3.61.0-gitlab) (2022-11-14)


### Features

* add log entry for repository size calculations ([f641948](https://gitlab.com/gitlab-org/container-registry/commit/f6419488c3b8cf3a2df58d105eef06ead8585d68))
* **auth:** parse auth_type value from jwt and expose in request logs ([39b15cf](https://gitlab.com/gitlab-org/container-registry/commit/39b15cf842bb0c3af815e526dad89dcfa105c503))
* **storage/driver/s3:** run DeleteFile batches in a loop instead of spawning goroutines ([39d999a](https://gitlab.com/gitlab-org/container-registry/commit/39d999a251742e5d210f119903daa78d0c3020be))

## [3.60.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.60.1-gitlab...v3.60.2-gitlab) (2022-11-08)


### Bug Fixes

* **gc:** graceful stop to offline gc if root repositories path non-exist ([41c8a5e](https://gitlab.com/gitlab-org/container-registry/commit/41c8a5e4a5f1cff7d7a379a5e7225d8fb4784084))

## [3.60.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.60.0-gitlab...v3.60.1-gitlab) (2022-10-28)


### Bug Fixes

* **handlers:** return 404 on v1 compliance check when metadata database is disabled ([eb44acf](https://gitlab.com/gitlab-org/container-registry/commit/eb44acfa0540d3010fd218c3675675bc3839ea54))

# [3.60.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.59.0-gitlab...v3.60.0-gitlab) (2022-10-25)


### Bug Fixes

* **s3:** treat s3 serialization error as retry-able ([#753](https://gitlab.com/gitlab-org/container-registry/issues/753)) ([f91a995](https://gitlab.com/gitlab-org/container-registry/commit/f91a995e1e504de73112c9658788a488fdc42c3b))


### Features

* **configuration:** add ability to log specific CF-ray header ([106bb9e](https://gitlab.com/gitlab-org/container-registry/commit/106bb9ef6e624ac778515cb7e2cee9a802a2bd4f))
* s3 driver support for ExistsPath ([37b8f72](https://gitlab.com/gitlab-org/container-registry/commit/37b8f723142be054ac96afe66bf8a88049a4e201))


### Performance Improvements

* temporary workaround for top-level namespace usage calculation ([3b3a19a](https://gitlab.com/gitlab-org/container-registry/commit/3b3a19a14006d503f7f0678a04b671705c034b4e))

# [3.59.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.58.0-gitlab...v3.59.0-gitlab) (2022-10-24)


### Features

* **configuration:** remove the ability to externally configure testslowimport ([27f9752](https://gitlab.com/gitlab-org/container-registry/commit/27f97522355396d82971914afe791bbaea0bd613))

# [3.58.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.57.0-gitlab...v3.58.0-gitlab) (2022-10-14)


### Features

* add new Prometheus metric for Redis connection pool size ([dfb2e15](https://gitlab.com/gitlab-org/container-registry/commit/dfb2e15b93fe4980445080c469053accbb35aeb0))

# [3.57.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.56.0-gitlab...v3.57.0-gitlab) (2022-08-16)


### Features

* **gc:** use statement-level trigger for tracking deleted layers ([4ae4c53](https://gitlab.com/gitlab-org/container-registry/commit/4ae4c53ac741ad6ff21e190a945452b1d821ac82))

# [3.56.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.55.0-gitlab...v3.56.0-gitlab) (2022-08-09)


### Bug Fixes

* **storage:** repositories must contain tags to be considered old in migration mode ([4baee1b](https://gitlab.com/gitlab-org/container-registry/commit/4baee1bbb58787dfcd130ff19b07d39578493dd6))


### Features

* **gc:** add statement-level trigger support for layer deletions ([4986d7e](https://gitlab.com/gitlab-org/container-registry/commit/4986d7ebbf904aa4052af8315e40ca5aeb0d6636))

# [3.55.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.54.0-gitlab...v3.55.0-gitlab) (2022-08-04)


### Features

* **gc:** add random jitter of 5 to 60 seconds to review due dates ([52c600c](https://gitlab.com/gitlab-org/container-registry/commit/52c600c7b6ae2036882fca59fdf80481b0ccb893))
* add support for http.debug.tls for monitoring service ([9d2eea9](https://gitlab.com/gitlab-org/container-registry/commit/9d2eea9743d39f820f72c2293c2553d52ee020a0))

# [3.54.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.53.0-gitlab...v3.54.0-gitlab) (2022-07-26)


### Features

* **datastore:** use tight timeout for Redis cache operations ([fd67535](https://gitlab.com/gitlab-org/container-registry/commit/fd67535677193d5162934c08fb7c197f39a05391))
* **gc:** add temporary debug log for deadlocks during manifest deletes ([841415b](https://gitlab.com/gitlab-org/container-registry/commit/841415b91d61fede5a983cf82083157c56f7872f))

# [3.53.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.52.0-gitlab...v3.53.0-gitlab) (2022-07-22)


### Bug Fixes

* **datastore:** gracefully handle missing Redis cache keys ([c72d743](https://gitlab.com/gitlab-org/container-registry/commit/c72d743bf8302059618cceeaa441782654524ddd))


### Features

* **api/gitlab/v1:** allow caching repositories in Redis for the get repository details operation ([aa39dfc](https://gitlab.com/gitlab-org/container-registry/commit/aa39dfc6149fefad01db46b316f349d69c9d1fb1))

# [3.52.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.51.1-gitlab...v3.52.0-gitlab) (2022-07-21)


### Features

* **datastore:** add ability to cache repository objects in Redis ([3a0e493](https://gitlab.com/gitlab-org/container-registry/commit/3a0e4931ffe5ac1374ce60813b14da20253effda))


### Reverts

* upgrade github.com/jackc/pgx/v4 from v4.13.0 to v4.16.1 ([6211766](https://gitlab.com/gitlab-org/container-registry/commit/6211766f7dd2ff44d0db888a517f4d1982642de6))

## [3.51.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.51.0-gitlab...v3.51.1-gitlab) (2022-07-15)


### Bug Fixes

* restore manifest delete webhook notifications on the new code path ([7012484](https://gitlab.com/gitlab-org/container-registry/commit/70124848a8b87f26783e806acb1f6030aa9b82fb))

# [3.51.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.50.0-gitlab...v3.51.0-gitlab) (2022-07-06)


### Features

* **importer:** skip reading manifest config if it exceeds limit ([323ca81](https://gitlab.com/gitlab-org/container-registry/commit/323ca815b8d9d9e94d9e2943855bc68ef48fb0b6))

# [3.50.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.49.0-gitlab...v3.50.0-gitlab) (2022-07-04)


### Features

* **api/gitlab/v1:** add new tag details list endpoint ([5a16e33](https://gitlab.com/gitlab-org/container-registry/commit/5a16e33f298b234966bf425120a6163232c763de))

# [3.49.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.48.0-gitlab...v3.49.0-gitlab) (2022-06-21)


### Bug Fixes

* **storage/driver/gcs:** getObject: do not return non-empty responses along with errors ([e1b3b47](https://gitlab.com/gitlab-org/container-registry/commit/e1b3b47b38aa9583571221e5f47a3584523ecf77))


### Features

* add support for ansible collection media type ([4bf5eeb](https://gitlab.com/gitlab-org/container-registry/commit/4bf5eeb97fc45fc58dd331c8bc4df89dc72ec648))
* add support for helm chart meta media type ([93fa792](https://gitlab.com/gitlab-org/container-registry/commit/93fa79213c6025112c4b0f7e9dbd950141cdddd3))
* **handlers:** attempt to cancel ongoing imports during graceful shutdown ([a18ef92](https://gitlab.com/gitlab-org/container-registry/commit/a18ef924d5531bf4015ae633261b54398c959405))

# [3.48.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.47.0-gitlab...v3.48.0-gitlab) (2022-06-10)


### Features

* retry pre import manifests due to transient errors ([b4ef1ff](https://gitlab.com/gitlab-org/container-registry/commit/b4ef1ff321709c60e58f55dad9546aa8b03e61df))


### Reverts

* temporarily gracefully handle nested lists during imports ([cbdaf09](https://gitlab.com/gitlab-org/container-registry/commit/cbdaf09a8b7ffe763ed468f2ac6ed65b02f0d2a0))

# [3.47.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.46.0-gitlab...v3.47.0-gitlab) (2022-06-07)


### Bug Fixes

* temporarily gracefully handle nested lists during imports ([204012c](https://gitlab.com/gitlab-org/container-registry/commit/204012c545c99ad338a66210d8a326c01237d6d8))


### Features

* add support for acme rocket media type ([5a85856](https://gitlab.com/gitlab-org/container-registry/commit/5a85856233e501de3a658c39715bf5a424fa7fa2))

# [3.46.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.45.0-gitlab...v3.46.0-gitlab) (2022-06-03)


### Bug Fixes

* allow buildkit indexes without layers ([6d2c356](https://gitlab.com/gitlab-org/container-registry/commit/6d2c356d1ccb7d9dbba7bb570f697a4f53364785))
* do not track skipped manifests during pre-imports ([dfde6ff](https://gitlab.com/gitlab-org/container-registry/commit/dfde6ffedb71d77656b2cc7f5dfd677944506f10))


### Features

* add support for additional gardener media types ([7626dd7](https://gitlab.com/gitlab-org/container-registry/commit/7626dd763aec7e638ef3bde221479be4c462b726))
* add support for additional misc media types ([13c34ce](https://gitlab.com/gitlab-org/container-registry/commit/13c34ce942d62a2c9727bfa57c67835a6942f3fc))
* add support for additional misc media types ([1ea61f6](https://gitlab.com/gitlab-org/container-registry/commit/1ea61f6e4b2449c2ab06ed01864309344f678078))

# [3.45.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.44.0-gitlab...v3.45.0-gitlab) (2022-06-01)


### Bug Fixes

* correctly handle embedded blob transfer errors ([1ec37dd](https://gitlab.com/gitlab-org/container-registry/commit/1ec37dd9c1e80e187a407c9c3287f0c4df87e079))
* skip import of broken/invalid manifest list/index references ([248904f](https://gitlab.com/gitlab-org/container-registry/commit/248904f8542b1dff59ec7a34ee0897b07c470b23))
* skip import of manifests with unlinked config blobs ([f65a3e6](https://gitlab.com/gitlab-org/container-registry/commit/f65a3e631b8597b598fe56e45495ebcc15ace887))


### Features

* add support for additional miscellaneous media types ([b315ac8](https://gitlab.com/gitlab-org/container-registry/commit/b315ac882777321a75a1801425f650ef27384607))

# [3.44.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.43.0-gitlab...v3.44.0-gitlab) (2022-05-31)


### Bug Fixes

* **datastore:** include digest and repository in unknown manifest class import error ([22ea61a](https://gitlab.com/gitlab-org/container-registry/commit/22ea61a6e39b88c410b6bda87a91af035dfa1f4f))


### Features

* add support for additional cosign media types ([dcd3e13](https://gitlab.com/gitlab-org/container-registry/commit/dcd3e135764a46c7e4a099b589f21516c4751ce3))
* add support for additional helm media type ([7b64fc7](https://gitlab.com/gitlab-org/container-registry/commit/7b64fc7b3f43168f0006bde3af356b34ad9370f0))
* add support for layer encryption media types ([079adfb](https://gitlab.com/gitlab-org/container-registry/commit/079adfb438be0382fffa3db8a840a45ed674566c))
* disable S3 MD5 header in FIPS mode ([42a82ab](https://gitlab.com/gitlab-org/container-registry/commit/42a82aba22f3e3479a24e083182b1b5f3be1c672))
* retry failed blob transfers due to timeouts once during imports ([da4a785](https://gitlab.com/gitlab-org/container-registry/commit/da4a78562a15f37b8a332a190120a1dd7821ca16))

# [3.43.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.42.0-gitlab...v3.43.0-gitlab) (2022-05-20)


### Bug Fixes

* gracefully handle missing manifest revisions during imports ([bc7c43f](https://gitlab.com/gitlab-org/container-registry/commit/bc7c43f30d8aba8f2edf2ca741b366614d9234c3))


### Features

* add ability to check/log whether FIPS crypto has been enabled ([1ac2454](https://gitlab.com/gitlab-org/container-registry/commit/1ac2454ac9dc7eeca5d9b555e0f1e6830fa66439))
* add support for additional gardener media types ([10153f8](https://gitlab.com/gitlab-org/container-registry/commit/10153f8df9a147806084aaff0f95a9d9536bbbe5))

# [3.42.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.41.1-gitlab...v3.42.0-gitlab) (2022-05-18)


### Bug Fixes

* restore manifest push (by tag) and tag delete webhook notifications ([e6a7984](https://gitlab.com/gitlab-org/container-registry/commit/e6a7984a6773fb138efe3a17d744c958249661ea))


### Features

* **storage:** improve clarity of offline garbage collection log output ([8b6129a](https://gitlab.com/gitlab-org/container-registry/commit/8b6129a3ca9fe81425a29540866ece795637ea05))

## [3.41.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.41.0-gitlab...v3.41.1-gitlab) (2022-05-13)


### Bug Fixes

* avoid logging a misleading warning on 401 response ([a36fb76](https://gitlab.com/gitlab-org/container-registry/commit/a36fb76bfd7fa4d53cf1ee00fadba58e5ff87833))
* **datastore:** attempt to import layers before importing manifests ([f95c638](https://gitlab.com/gitlab-org/container-registry/commit/f95c6385e55ca584eae5259961c63819608388fc))
* **datastore:** do not attempt to import manifests with empty layer links ([a1bf813](https://gitlab.com/gitlab-org/container-registry/commit/a1bf8135021734bbc896361688268300020fa4fd))
* **datastore:** prevent canceled (pre) imports being marked (pre_)import_complete ([a224f0b](https://gitlab.com/gitlab-org/container-registry/commit/a224f0b96f3aec8f36c790e3729388825b674b3e))
* **distribution:** prevent nil cleanup errors in ErrBlobTransferFailed from causing panics ([25ebe91](https://gitlab.com/gitlab-org/container-registry/commit/25ebe91a5b522bfcdbc34f21daad91221b2430df))
* **handlers:** allow enough time for imports to be canceled before additional pre import attempts ([0deddeb](https://gitlab.com/gitlab-org/container-registry/commit/0deddeb06b194771f8493afff95432126ef3e4bb))
* **importer:** handle buildkit index as manifest ([774b9ef](https://gitlab.com/gitlab-org/container-registry/commit/774b9ef7cc033fdb593b980194b5c34c28e97640))
* **storage/driver/gcs:** use CRC32C checksums to validate transferred blobs ([f566216](https://gitlab.com/gitlab-org/container-registry/commit/f56621621d6a0fe0a4f6cbedf96546c67c208850))

# [3.41.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.40.0-gitlab...v3.41.0-gitlab) (2022-05-05)


### Bug Fixes

* allow query aggregated size of repositories under an unknown base path ([89c2d3b](https://gitlab.com/gitlab-org/container-registry/commit/89c2d3b0cb9d37a4579325f0ba46f2c52e3fc49a))
* **datastore:** do not ignore last tag lookup error when importing tags ([4b149b2](https://gitlab.com/gitlab-org/container-registry/commit/4b149b23b323b80abdcbbb425b10fcb803aeb831))


### Features

* add support for additional WASM media types ([e8c58c8](https://gitlab.com/gitlab-org/container-registry/commit/e8c58c80f99b0327f33b30e090b94f6c72c7a25a))
* add support for Gardener media types ([967b1a5](https://gitlab.com/gitlab-org/container-registry/commit/967b1a551e4335c09a01e4b2c5691c2076462c33))

# [3.40.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.39.3-gitlab...v3.40.0-gitlab) (2022-05-04)


### Bug Fixes

* add missing Close call for serveral resources ([78d0aaf](https://gitlab.com/gitlab-org/container-registry/commit/78d0aafcdfae94e3bd6a76661c42a62419baaf51))
* gracefully handle missing tag links during final imports ([3cdbfa3](https://gitlab.com/gitlab-org/container-registry/commit/3cdbfa307b4e38aacf033d2daab2f06294369ebd))


### Features

* add support for ArtifactHUB media types ([d58f4b3](https://gitlab.com/gitlab-org/container-registry/commit/d58f4b3778e1e4f3df999ce32be9dd827b4c5efb))
* add support for CNAB media types ([986b213](https://gitlab.com/gitlab-org/container-registry/commit/986b2132efcaf07a04302c39eaca8562f4f83b97))
* add support for cosign media types ([247849e](https://gitlab.com/gitlab-org/container-registry/commit/247849e6c014f25ef4ddac528453cfbab8b8a901))
* add support for OPA media types ([73c83ff](https://gitlab.com/gitlab-org/container-registry/commit/73c83ffc7e5ef39a5917a427f691d0f831a5d73d))
* add support for SIF media types ([d80268e](https://gitlab.com/gitlab-org/container-registry/commit/d80268ed969058a962f854745df62931500f30df))
* add support for WASM media types ([210317a](https://gitlab.com/gitlab-org/container-registry/commit/210317ab53b21e3eae9243f5a360319c519a2814))

## [3.39.3](https://gitlab.com/gitlab-org/container-registry/compare/v3.39.2-gitlab...v3.39.3-gitlab) (2022-05-02)


### Bug Fixes

* **datastore:** do not attempt to import non-distributable layers ([7449428](https://gitlab.com/gitlab-org/container-registry/commit/74494287abdcdbe99841424970a5363426e4b10b))
* **datastore:** gracefully handle manifest broken link during imports ([32bc992](https://gitlab.com/gitlab-org/container-registry/commit/32bc9924bb56b676dd00efd13efefe890686be26))
* **datastore:** gracefully handle tags deleted between list and import ([1c1a6d6](https://gitlab.com/gitlab-org/container-registry/commit/1c1a6d6fa5e3c589d1e3185a5167681017da138f))
* **datastore:** gracefully handle tags with broken links during imports ([2b53c0d](https://gitlab.com/gitlab-org/container-registry/commit/2b53c0d3ff9adbfcfb77fb12c7a0ef1bf77c13f4))
* **datastore:** race condition pushing two or more manifests by tag with the same digest ([54accd5](https://gitlab.com/gitlab-org/container-registry/commit/54accd53272b211046538aec5c23c008c35cfb86))
* **handlers:** clarify log message when registry is at max concurrent imports ([aa3825c](https://gitlab.com/gitlab-org/container-registry/commit/aa3825c662596f9772ef61c356faff75566e5c01))

## [3.39.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.39.1-gitlab...v3.39.2-gitlab) (2022-04-22)


### Bug Fixes

* always fully pre-import manifests once, even on retries ([0152b7b](https://gitlab.com/gitlab-org/container-registry/commit/0152b7bf4d091f05300d297ac9ebbfaf9f357398))
* Added missing error checking on S3 driver delete operation #551([5aa8995](https://gitlab.com/gitlab-org/container-registry/-/commit/5aa89957a002e2bad37630a21c8561f2e77f52a3))


### Reverts

* gracefully handle missing manifest revisions during imports ([a597466](https://gitlab.com/gitlab-org/container-registry/commit/a59746686641d60dcd8625995a5ba0e781e17005))

## [3.39.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.39.0-gitlab...v3.39.1-gitlab) (2022-04-20)


### Bug Fixes

* gracefully handle missing manifest revisions during imports ([dc31a55](https://gitlab.com/gitlab-org/container-registry/commit/dc31a55756bdb66e3a1298fc43f3ae8e3e6466a7))
* gracefully handle soft-deleted repositories during imports ([f74d5e2](https://gitlab.com/gitlab-org/container-registry/commit/f74d5e2e56201fcc2860d86a59da595ac46efb22))
* gracefully handle unsupported schema v1 manifests during imports ([7e408f8](https://gitlab.com/gitlab-org/container-registry/commit/7e408f88357a11e69cd811b95f8adb4aeb5b6ddf))

# [3.39.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.38.0-gitlab...v3.39.0-gitlab) (2022-04-13)


### Bug Fixes

* properly initialize multierror during imports ([3e33781](https://gitlab.com/gitlab-org/container-registry/commit/3e33781a7443a8093bd4069fe8ecaab0b260b6ed))


### Features

* add histogram for counting imported layers per manifest ([7fea14a](https://gitlab.com/gitlab-org/container-registry/commit/7fea14a53021987e4004d514abe6c2f9e340aae4))
* add histogram metric to monitor the number of imported tags ([4eeab85](https://gitlab.com/gitlab-org/container-registry/commit/4eeab85a1a56a30dc68fa06789709f1f28c9d397))
* add histograms for blob transfer durations and byte sizes ([dc959bb](https://gitlab.com/gitlab-org/container-registry/commit/dc959bbd1bd5134db9afd973fecc6ef7250237be))

# [3.38.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.37.1-gitlab...v3.38.0-gitlab) (2022-04-07)


### Bug Fixes

* cleanup migration error when retrying a failed import ([b749274](https://gitlab.com/gitlab-org/container-registry/commit/b749274aa963918735e105b13d7839ea6dfdcccc))
* **handlers:** update import status when failed with a different context ([e3e7c1f](https://gitlab.com/gitlab-org/container-registry/commit/e3e7c1fc7a30ccb943a4c9c8eee20a3385d957de))


### Features

* **handlers:** expose migration_error to GET import route ([37af549](https://gitlab.com/gitlab-org/container-registry/commit/37af5498f84d474cb1f213da8511c69795f9ce79))
* **handlers:** force final import cancellation ([becaea6](https://gitlab.com/gitlab-org/container-registry/commit/becaea690359a5328d363ec4506be9213453b22a))

## [3.37.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.37.0-gitlab...v3.37.1-gitlab) (2022-04-04)


### Bug Fixes

* gracefully handle missing tags prefix during imports ([6041785](https://gitlab.com/gitlab-org/container-registry/commit/6041785ba03117fed341cc614ee479ba23d52c6c))

# [3.37.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.36.1-gitlab...v3.37.0-gitlab) (2022-03-30)


### Bug Fixes

* do not report import error if target repository does not exist ([2775446](https://gitlab.com/gitlab-org/container-registry/commit/2775446b9b3a15d0a1028fdf3f85e01039336ec6))
* gracefully handle missing tags prefix during pre-imports ([a49aa80](https://gitlab.com/gitlab-org/container-registry/commit/a49aa80be642beb01dd0daf9588bb6437d374447))
* skip empty manifest during (pre)import ([97effe0](https://gitlab.com/gitlab-org/container-registry/commit/97effe0e314b27789c1bd147bf1d2cf6210579ad))


### Features

* implement import DELETE endpoint ([6edc99b](https://gitlab.com/gitlab-org/container-registry/commit/6edc99b316bba098c120a0196e109e0d71796bc8))

## [3.36.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.36.0-gitlab...v3.36.1-gitlab) (2022-03-22)


### Bug Fixes

* set response content-type for get import status requests ([56509fa](https://gitlab.com/gitlab-org/container-registry/commit/56509fab29927845fc664278b99c36e663251ad3))

# [3.36.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.35.0-gitlab...v3.36.0-gitlab) (2022-03-21)


### Bug Fixes

* bypass blob get notifications to avoid filesystem link check in migration mode ([c87899b](https://gitlab.com/gitlab-org/container-registry/commit/c87899b90e4b3aafa2f16080bc57dae3dcb8f554))
* increment and decrement inflight imports metric correctly ([9766063](https://gitlab.com/gitlab-org/container-registry/commit/9766063232e309c780bff29965c33799e2594b4b))
* **handlers:** always require import type query param to be present ([e80c655](https://gitlab.com/gitlab-org/container-registry/commit/e80c65581005eb328d10e6b95fc02735f973dfb4))
* **handlers:** do not allow final import without a preceding pre import ([44f7382](https://gitlab.com/gitlab-org/container-registry/commit/44f7382f3445fe52cbf912a5777bd26491fda836))


### Features

* add support for Singularity media types ([a78b5d8](https://gitlab.com/gitlab-org/container-registry/commit/a78b5d8a42f0fb8aa895333ea659a623175ec7c3))
* log whether blob transfer was a noop during repository import ([f290f47](https://gitlab.com/gitlab-org/container-registry/commit/f290f473572fee7d6e034b14b966d3e640e39dbe))

# [3.35.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.34.0-gitlab...v3.35.0-gitlab) (2022-03-15)


### Bug Fixes

* abort import if layer has unknown media type ([d9a4779](https://gitlab.com/gitlab-org/container-registry/commit/d9a4779d6f024e5e80b4fda8d3adad1f92d0c6fc))
* **handlers:** release concurrency semaphore on noop imports ([08099ba](https://gitlab.com/gitlab-org/container-registry/commit/08099ba420da776e850bae36e8d604ef187ee074))


### Features

* parse message from Rails in failed migration notification ([cbbda65](https://gitlab.com/gitlab-org/container-registry/commit/cbbda65d064ab317910245e8d9d723de5456b908))
* **datastore:** add component key/value pair to Importer log entries ([14fc6ed](https://gitlab.com/gitlab-org/container-registry/commit/14fc6edb1e9d1211e0cb95e8094f5966dd3094dc))
* **datastore:** index soft-deleted repository records ([88aac62](https://gitlab.com/gitlab-org/container-registry/commit/88aac622a25723da220a6b5f02fc94562a7811a5))
* **handler:** update migration_error and use value in import notification ([73a00c0](https://gitlab.com/gitlab-org/container-registry/commit/73a00c024932a0064dbce81e37e7d20c165221d4))


### Reverts

* extra debug logging for GCS stat and blob transfer service ([845fbc2](https://gitlab.com/gitlab-org/container-registry/commit/845fbc2201a721cf1ef1c285e45d6f94935fb06b))

# [3.34.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.33.0-gitlab...v3.34.0-gitlab) (2022-03-08)


### Bug Fixes

* **handlers:** prevent middleware from interrupting blob transfer ([0b9dab1](https://gitlab.com/gitlab-org/container-registry/commit/0b9dab1d5b25285c4839b0f0a4a7121283a0c4d4))
* GCS driver TransferTo and integration tests ([a9779c1](https://gitlab.com/gitlab-org/container-registry/commit/a9779c1917b7379dcbae4cfc67931d7f5eb38203))


### Features

* add metrics for max concurrent imports ([269c3f6](https://gitlab.com/gitlab-org/container-registry/commit/269c3f6571d807dd57039c8482e9db5e5cfbadfa))

# [3.33.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.32.0-gitlab...v3.33.0-gitlab) (2022-03-07)


### Bug Fixes

* **storage:** add extra debug logging for GCS stat and blob transfer service ([3366b1d](https://gitlab.com/gitlab-org/container-registry/commit/3366b1d067984c3b362473b4d1c212193f7f21af))


### Features

* **storage:** instrument '429 Too Many Requests' responses ([9156bb9](https://gitlab.com/gitlab-org/container-registry/commit/9156bb9fd510f667f8e67870f8a5cf79292505a2))

# [3.32.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.31.0-gitlab...v3.32.0-gitlab) (2022-03-04)


### Bug Fixes

* **storage:** check for nil drivers before creating blob transfer service ([8bf377f](https://gitlab.com/gitlab-org/container-registry/commit/8bf377f16f08ddbd106fb110ba8392faff634f47))


### Features

* add queries and methods to calculate the deduplicated size of nested repositories ([2666074](https://gitlab.com/gitlab-org/container-registry/commit/26660744e8264e76691685c4e78b0a58f1eae6a9))
* **api/gitlab/v1:** calculate size of base repository and its descendants ([b804875](https://gitlab.com/gitlab-org/container-registry/commit/b80487533915e4b13813970471f707e69c2722ce))
* deprecate htpasswd authentication ([81a3bd2](https://gitlab.com/gitlab-org/container-registry/commit/81a3bd274add2279321dab97437fb8dea674cbde))
* **handlers:** expose Migration.TestSlowImport in config ([7017917](https://gitlab.com/gitlab-org/container-registry/commit/7017917c4b3820dce5379c0136e0918ea36162db))
* **handlers:** reject write requests during repository import ([3096022](https://gitlab.com/gitlab-org/container-registry/commit/3096022da7cb5ef8860910724a3ab42d5df732f9))

# [3.31.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.30.0-gitlab...v3.31.0-gitlab) (2022-03-01)


### Bug Fixes

* enforce single access record for import route auth tokens ([c476307](https://gitlab.com/gitlab-org/container-registry/commit/c476307fd5a6509dc78ccbe76fc87f33dc7b5bfe))
* use unique value for /gitlab/v1 API route names on metrics ([8e6550a](https://gitlab.com/gitlab-org/container-registry/commit/8e6550ac0eeb6754ec01a91fae0c92e9b8d7f24e))
* **handlers:** return a proper error code for invalid import query value ([caabf19](https://gitlab.com/gitlab-org/container-registry/commit/caabf192881b13cdc7db5759e16d6b74a0832af6))


### Features

* add support for custom oras media types ([625f7a3](https://gitlab.com/gitlab-org/container-registry/commit/625f7a3029da451286dfc3096260c132826a8e2f))
* **migration:** use Seperate Context for Import Notifications ([62bf886](https://gitlab.com/gitlab-org/container-registry/commit/62bf8867134e5c262d3699132230d9da2f9c6b26))
* implement import GET route ([d67ba80](https://gitlab.com/gitlab-org/container-registry/commit/d67ba800d2ee2a4c22569ba72d869a0d8e5e0000))

# [3.30.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.29.0-gitlab...v3.30.0-gitlab) (2022-02-23)


### Features

* **handlers:** use phase 2 code routing for API requests ([3e3b9c3](https://gitlab.com/gitlab-org/container-registry/commit/3e3b9c3eec80b14f4c9260751a1fc12c988b8b1c))

# [3.29.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.28.2-gitlab...v3.29.0-gitlab) (2022-02-22)


### Features

* **handlers:** rename pre parameter on import route ([4540965](https://gitlab.com/gitlab-org/container-registry/commit/45409652bbccf167f015f89dabca732fb8cf6c71))
* serve requests for all new repositories using the new code path ([e4a1472](https://gitlab.com/gitlab-org/container-registry/commit/e4a14726eecae0628f16d615c4df265bdb70ccc8))

## [3.28.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.28.1-gitlab...v3.28.2-gitlab) (2022-02-18)


### Bug Fixes

* soft delete empty repository records (batch 6) ([415d83d](https://gitlab.com/gitlab-org/container-registry/commit/415d83d146e6f43d684aca559ce827cbd9413cd4))
* **storage:** refuse to start offline garbage collection with database metadata enabled ([805f21c](https://gitlab.com/gitlab-org/container-registry/commit/805f21c0a7bdfbd5785ada4979d3136658a93539))

## [3.28.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.28.0-gitlab...v3.28.1-gitlab) (2022-02-18)


### Bug Fixes

* soft delete empty repository records (batch 5) ([8d0d36d](https://gitlab.com/gitlab-org/container-registry/commit/8d0d36df94769dd44a88aa2504e3c366c2bc0081))

# [3.28.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.27.1-gitlab...v3.28.0-gitlab) (2022-02-17)


### Bug Fixes

* soft delete empty repository records (batch 4) ([e6e3252](https://gitlab.com/gitlab-org/container-registry/commit/e6e32528159d8b86009c09212d884d0d876c2d84))


### Features

* **notifier:** forward correlation ID for import notification ([b99cb86](https://gitlab.com/gitlab-org/container-registry/commit/b99cb86b117b7943ca8023c85b571b644d59e786))

## [3.27.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.27.0-gitlab...v3.27.1-gitlab) (2022-02-16)


### Bug Fixes

* soft delete empty repository records (batch 3) ([3417b00](https://gitlab.com/gitlab-org/container-registry/commit/3417b00aa361d7affe76138507e3091dc4f6342c))

# [3.27.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.26.0-gitlab...v3.27.0-gitlab) (2022-02-16)


### Bug Fixes

* soft delete empty repository records (batch 2) ([b176df0](https://gitlab.com/gitlab-org/container-registry/commit/b176df0823882f96e87c3b36f33d5ad631b0b6d5))


### Features

* **datastore:** add support for repository options on creation ([fbb7753](https://gitlab.com/gitlab-org/container-registry/commit/fbb77531f684c0cac88493a464621612fa445bb2))

# [3.26.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.25.0-gitlab...v3.26.0-gitlab) (2022-02-15)


### Bug Fixes

* soft delete empty repository records (batch 1) ([8e58b4d](https://gitlab.com/gitlab-org/container-registry/commit/8e58b4de799894cd2a2a0d40f93cad341a2f5b41))


### Features

* do not limit redirections to Google Cloud CDN ([ec64f51](https://gitlab.com/gitlab-org/container-registry/commit/ec64f51faf49d785083f6d74fca054d33a1ac9a9))
* **handlers:** add support for maxconcurrentimports in the import handler ([3b789ae](https://gitlab.com/gitlab-org/container-registry/commit/3b789aeaca05c10d2a5b316290b60a09d4c0b98c))
* **log/context:** log registry version everywhere ([bdd2844](https://gitlab.com/gitlab-org/container-registry/commit/bdd284460ef703bdaf3988862cfe5e6765ee2d60))

# [3.25.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.24.1-gitlab...v3.25.0-gitlab) (2022-02-11)


### Bug Fixes

* ignore soft-deleted repositories on reads and undo soft-delete on writes ([47045dd](https://gitlab.com/gitlab-org/container-registry/commit/47045dd557694fb1490d87f87f07b176b6c6fc35))


### Features

* **configuration:** enable setting (pre) import timeoutes for the API import route ([319b04a](https://gitlab.com/gitlab-org/container-registry/commit/319b04a145af8c4001965cf53b0ef64b8015487b))

## [3.24.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.24.0-gitlab...v3.24.1-gitlab) (2022-02-09)


### Bug Fixes

* stop creating intermediate repositories and parent/child links ([f49eae4](https://gitlab.com/gitlab-org/container-registry/commit/f49eae424252d839fdaf851fbcdc989af955e396))

# [3.24.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.23.0-gitlab...v3.24.0-gitlab) (2022-02-09)


### Bug Fixes

* **datastore:** importer: do not pass on manifest errors ([37b7db2](https://gitlab.com/gitlab-org/container-registry/commit/37b7db22934335ab50e23197161696e8b565f861))
* **datastore:** remove unnecessary transaction for manifest pre import ([1dd2e30](https://gitlab.com/gitlab-org/container-registry/commit/1dd2e306af4a3e599eed33332df4a8896b8e1130))
* **datastore:** repositorystore Update updates migration status ([2c1e96f](https://gitlab.com/gitlab-org/container-registry/commit/2c1e96f4e5b369baabca11e42f3bb7169fc6b5fa))
* halt (pre)import on invalid manifest referenced by list ([1567594](https://gitlab.com/gitlab-org/container-registry/commit/15675942972e50f8adb345826de65cd62d3a28d9))
* halt import on tag lookup failure ([c10f2e5](https://gitlab.com/gitlab-org/container-registry/commit/c10f2e548910bfb9eda19990d66c0cbe15917688))
* halt pre-import on tag lookup failure ([002dab7](https://gitlab.com/gitlab-org/container-registry/commit/002dab7749a9fab1cbd70233fa75babb3f525c55))
* **handlers:** handle runImport error ([f292acd](https://gitlab.com/gitlab-org/container-registry/commit/f292acd568e3ef18ba503dc3fe99b6ce1e0a9dc9))
* **migration:** parse placeholder for path in import notifier endpoint ([621dc4e](https://gitlab.com/gitlab-org/container-registry/commit/621dc4eb104709c563c8e16a66c766510d5b93da))
* **migration:** typo in import failed status ([7fbd392](https://gitlab.com/gitlab-org/container-registry/commit/7fbd392eac7fb1bd77fad1435524dc4df9141aec))


### Features

* **datastore:** use context fields for importer logging ([83e783c](https://gitlab.com/gitlab-org/container-registry/commit/83e783c8dc45a45007d3654f96b5a442b02ad642))
* **handlers:** import route: return 409 conflict if repository is importing ([c11fd43](https://gitlab.com/gitlab-org/container-registry/commit/c11fd43a3f7f095138472d5fb3c0ec8430583ee0))
* **handlers:** import route: return 424 failed dependency if repository failed previous pre import ([9d7c3b4](https://gitlab.com/gitlab-org/container-registry/commit/9d7c3b4b3b2e17b0b003927bb89fa48fd958141d))
* **handlers:** import route: return 425 too early if repository is pre importing ([13b67de](https://gitlab.com/gitlab-org/container-registry/commit/13b67de8a6289c2ca966b1eabb2613c16b6c5bf7))
* **handlers:** metrics for API import route ([55fb814](https://gitlab.com/gitlab-org/container-registry/commit/55fb81431d7bf1ad928867dd32f93a2975b79739))
* **handlers:** send import notifications ([9827815](https://gitlab.com/gitlab-org/container-registry/commit/9827815f408c19bc8b2e6e2106ad58a05b55d255))
* **handlers:** update repository migration status during import ([062a47c](https://gitlab.com/gitlab-org/container-registry/commit/062a47c26e6f5dacbf60c2bf6799e3e6d3a21c33))
* make the importer row count logging opt in ([a1c476a](https://gitlab.com/gitlab-org/container-registry/commit/a1c476a4d3c724bf7df6f1ba907172d6315f027a))

# [3.23.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.22.0-gitlab...v3.23.0-gitlab) (2022-01-20)


### Features

* **api/gitlab/v1:** implement get repository details operation ([d2e92b9](https://gitlab.com/gitlab-org/container-registry/commit/d2e92b9f58600f8f35349fcb010dca3d53759aae))
* **handlers:** enable pre-imports via the Gitlab V1 API ([967a0c9](https://gitlab.com/gitlab-org/container-registry/commit/967a0c9d8bf5fc45ac3ff43da6bfd4ea77b92546))

# [3.22.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.21.0-gitlab...v3.22.0-gitlab) (2022-01-13)


### Features

* track online migration status of repositories in the database ([a18b11b](https://gitlab.com/gitlab-org/container-registry/commit/a18b11bd9fe27eaa2310160d0cd14a8440d5db07))
* **configuration:** add tagconcurrency to migration stanza ([fcc4595](https://gitlab.com/gitlab-org/container-registry/commit/fcc4595833a75972b5b553597ff0b5c987e7ecf6))
* **handlers:** add gitlab v1 API import route ([beba40e](https://gitlab.com/gitlab-org/container-registry/commit/beba40ec4d513730aed63fb1544b3502a7ad7f2a))
* **handlers:** add support for tag concurrency to import API route ([1bf25b3](https://gitlab.com/gitlab-org/container-registry/commit/1bf25b3f0e9558de0a97bf6251fb6b3045e7ca7d))
* **handlers:** send import errors to sentry ([968a027](https://gitlab.com/gitlab-org/container-registry/commit/968a02736ff3d6c2d0761f672399a5aa3d0703c1))
* limit redirections to Google Cloud CDN based on a feature flag ([feb1604](https://gitlab.com/gitlab-org/container-registry/commit/feb160454437c1a2aa9ec8f1be12a32b4951bcb8))

# [3.21.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.20.0-gitlab...v3.21.0-gitlab) (2022-01-06)


### Bug Fixes

* correct typo cloudfront updatefrenquency ([dddf7aa](https://gitlab.com/gitlab-org/container-registry/commit/dddf7aa09e5a5ac4c6dfacff03a89c799e8df524))
* handle missing foreign layers gracefully ([ddb578a](https://gitlab.com/gitlab-org/container-registry/commit/ddb578a1196276fc96686d23b32c1836ae3dce06))


### Features

* **handlers:** add gitlab v1 API base route ([6efb384](https://gitlab.com/gitlab-org/container-registry/commit/6efb384974f63a3aca6e6d5a22c0456965fc6e3e))

# [3.20.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.19.0-gitlab...v3.20.0-gitlab) (2021-12-30)


### Bug Fixes

* **datastore:** check for blob access before importing layer ([84cf639](https://gitlab.com/gitlab-org/container-registry/commit/84cf639925de58dc88a341db8cb3f400c5a2b21f))
* **datastore:** skip caching of large configuration payloads ([748dbaa](https://gitlab.com/gitlab-org/container-registry/commit/748dbaa4a344bd83b5205f3e3c8666cb80626772))
* remove temporary log entries for "finding repository by path" queries ([090e34c](https://gitlab.com/gitlab-org/container-registry/commit/090e34cde19994dd17e6f381eed2ab433140fe2c))


### Features

* add Google CDN support ([612e861](https://gitlab.com/gitlab-org/container-registry/commit/612e8619befeeccb43ae448cdfd8e454834e9224))
* **handlers/configuration:** enable manifest payload size limit ([db18ba1](https://gitlab.com/gitlab-org/container-registry/commit/db18ba18115c452b09d3e5c70d46d32a40dde5e6))

# [3.19.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.18.1-gitlab...v3.19.0-gitlab) (2021-12-17)


### Features

* **datastore:** add not null constraint to event column on GC queues ([e489c0d](https://gitlab.com/gitlab-org/container-registry/commit/e489c0d1a324ed1aa891faa9e3e340765b93ff16))
* **gc:** add event label to online GC run metrics ([552b83f](https://gitlab.com/gitlab-org/container-registry/commit/552b83f5efd35db56f706e02941840b225deae0d))

## [3.18.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.18.0-gitlab...v3.18.1-gitlab) (2021-12-10)


### Bug Fixes

* revert enable PostgreSQL pageinspect extension ([2c2825c](https://gitlab.com/gitlab-org/container-registry/commit/2c2825cacef5f631f72aedbee0f53318a6c846c5))

# [3.18.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.17.0-gitlab...v3.18.0-gitlab) (2021-12-09)


### Bug Fixes

* **datastore:** do not panic if database credentials are wrong ([cfc51e7](https://gitlab.com/gitlab-org/container-registry/commit/cfc51e79846bfd03580f0b42a6c25e33b70b9478))


### Features

* **datastore:** calculate deduplicated repository size ([#486](https://gitlab.com/gitlab-org/container-registry/issues/486)) ([86d68c1](https://gitlab.com/gitlab-org/container-registry/commit/86d68c1a4d6c8fe0fec3ad8b41db7af656d1ffba))
* **datastore:** enable PostgreSQL pageinspect extension ([74f6521](https://gitlab.com/gitlab-org/container-registry/commit/74f65217b10e33ed3a43ddd972b5bf23083b5405))
* **datastore:** extend support for Helm Charts media types ([ff2fd80](https://gitlab.com/gitlab-org/container-registry/commit/ff2fd80e12d7a92b5fbdbd1ff184b2cc9feac02f))
* **gc:** add dangling and event labels to online GC run metrics ([738cf24](https://gitlab.com/gitlab-org/container-registry/commit/738cf2474aa2299613566853dc00e630bafbdb1c))
* **gc:** log creation timestamp and event of GC tasks ([e0133c3](https://gitlab.com/gitlab-org/container-registry/commit/e0133c3b3c28aa774fa43b30e33726f7e1b55327))

# [3.17.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.16.0-gitlab...v3.17.0-gitlab) (2021-11-22)


### Bug Fixes

* **handlers:** disable upload purging if read-only mode is enabled ([#169](https://gitlab.com/gitlab-org/container-registry/issues/169)) ([6f24d30](https://gitlab.com/gitlab-org/container-registry/commit/6f24d301765d92af7b29690fff80067010544b1d))


### Features

* **gc:** track event that led to creation of an online GC blob review task ([1354996](https://gitlab.com/gitlab-org/container-registry/commit/1354996332547609622f202adce7aed800245646))

# [3.16.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.15.0-gitlab...v3.16.0-gitlab) (2021-11-18)


### Features

* **gc:** record event type for manifest tasks queued due to a tag delete or switch ([e0d918a](https://gitlab.com/gitlab-org/container-registry/commit/e0d918ab1d1276cf4aa848f54252ba5fa3d54a2f))
* **gc:** record event type for manifest tasks queued due to list delete ([68828ea](https://gitlab.com/gitlab-org/container-registry/commit/68828ea1f857027a64aa41e728b0d055706798ad))

# [3.15.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.14.3-gitlab...v3.15.0-gitlab) (2021-11-16)


### Features

* **gc:** improve logging for artifact deletions ([4d77d47](https://gitlab.com/gitlab-org/container-registry/commit/4d77d473ba9539bad8f4049b84748fcce697dbc7))
* **gc:** record event type for manifest tasks queued due to an upload ([1b2a534](https://gitlab.com/gitlab-org/container-registry/commit/1b2a53467bda396c7893eadc3e636ad3bdb70e2c))
* **handlers:** temporarily log details of "find repository by path" queries ([8a3fcca](https://gitlab.com/gitlab-org/container-registry/commit/8a3fcca3cc0da95d8a76ba301da270b557c0fa04))
* **storage:** add Prometheus histogram metric for new blob uploads ([8a9c4a0](https://gitlab.com/gitlab-org/container-registry/commit/8a9c4a01f07e4eb98dbcb18891b676162ffcf5d5))

## [3.14.3](https://gitlab.com/gitlab-org/container-registry/compare/v3.14.2-gitlab...v3.14.3-gitlab) (2021-11-09)


### Bug Fixes

* **datastore:** use "safe find or create" instead of "create or find" for namespaces ([0feff9e](https://gitlab.com/gitlab-org/container-registry/commit/0feff9edcd99fd7afc9a5c5e71fc1e161915e36d))
* **datastore:** use "safe find or create" instead of "create or find" for repositories ([7b73cc9](https://gitlab.com/gitlab-org/container-registry/commit/7b73cc986d8855e42d8890801d62bb82b9c07df5))

## [3.14.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.14.1-gitlab...v3.14.2-gitlab) (2021-11-03)


### Bug Fixes

* **gc:** commit database transaction when no task was found ([2f4e2f9](https://gitlab.com/gitlab-org/container-registry/commit/2f4e2f949b3194358eb9dd3a0b5eb49a8b0d9398))

## [3.14.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.14.0-gitlab...v3.14.1-gitlab) (2021-10-29)


### Performance Improvements

* **handlers:** improve performance of repository existence check for GCS ([e31e5ed](https://gitlab.com/gitlab-org/container-registry/commit/e31e5ed5993ca8496e6801a4f833100e85f5f005))

# [3.14.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.13.0-gitlab...v3.14.0-gitlab) (2021-10-28)


### Bug Fixes

* **handlers:** do not log when blob or manifest HEAD requests return not found errors ([0f407e3](https://gitlab.com/gitlab-org/container-registry/commit/0f407e3cebc933cc908109a52417ff89501998fa))
* **handlers:** use 503 Service Unavailable for DB connection failures ([fecb78d](https://gitlab.com/gitlab-org/container-registry/commit/fecb78d804d3b5717c93567da3fe4a000dc68630))


### Features

* **handlers:** log when migration status is determined ([75b8230](https://gitlab.com/gitlab-org/container-registry/commit/75b8230ad3080d576f6b7564c29e435c3f0e1d0e))
* **handlers/configuration:** enable enforcing manifest reference limits ([2154e73](https://gitlab.com/gitlab-org/container-registry/commit/2154e7308f863ec27a8c454ca60a859afc9b4fd5))

# [3.13.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.12.0-gitlab...v3.13.0-gitlab) (2021-10-14)


### Bug Fixes

* update Dockerfile dependencies to allow successful builds ([3dc2f1a](https://gitlab.com/gitlab-org/container-registry/commit/3dc2f1a29270534a65daff4b986a75ff2bbd87f7))


### Features

* **configuration:** use structured logging in configuration parser ([49d7d10](https://gitlab.com/gitlab-org/container-registry/commit/49d7d10116836eacdacc44738f7123f6ceebe5ae))
* **datastore/handlers:** cache repository objects in memory for manifest PUT requests ([66bd599](https://gitlab.com/gitlab-org/container-registry/commit/66bd599dd16a2fc3046f58325c165be312783088))

# [3.12.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.11.1-gitlab...v3.12.0-gitlab) (2021-10-11)


### Bug Fixes

* **handlers:** only log that a manifest/blob was downloaded if the method is GET ([19d9f60](https://gitlab.com/gitlab-org/container-registry/commit/19d9f608960990dbfe12209f5743ac30776b1988))


### Features

* **datastore:** add created_at timestamp to online GC review queue tables ([3a38a0a](https://gitlab.com/gitlab-org/container-registry/commit/3a38a0aef96d607730b8ba0d728d702477c32331))
* **datastore:** calculate total manifest size on creation ([4c38d53](https://gitlab.com/gitlab-org/container-registry/commit/4c38d53adc3fd09fd9f899f90ab0c34b375143be))
* **handlers:** log metadata when a new blob is uploaded ([83cb07e](https://gitlab.com/gitlab-org/container-registry/commit/83cb07e98c55ee1eee00c5c31b9c668d59a7ba22))

## [3.11.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.11.0-gitlab...v3.11.1-gitlab) (2021-09-20)


### Bug Fixes

* **api/errcode:** extract enclosed error from a storage driver catch-all error ([800a15e](https://gitlab.com/gitlab-org/container-registry/commit/800a15e282f79ed41ef0c3606a3303224cd176d1))
* **api/errcode:** propagate 503 Service Unavailable status thrown by CGS ([21041fe](https://gitlab.com/gitlab-org/container-registry/commit/21041fef6f3e863d598d6bb97476815ae2518e38))
* **gc:** always propagate correlation ID from agent to workers ([8de9c93](https://gitlab.com/gitlab-org/container-registry/commit/8de9c936d52e4ab84bb40efae63ea15371d70bc3))
* **gc/worker:** delete task if dangling manifest no longer exists on database ([dffdd72](https://gitlab.com/gitlab-org/container-registry/commit/dffdd72d6527dfc037fc6bcbda2530ac83c9fe4b))
* **handlers:** ignore tag not found errors when deleting a manifest ([e740416](https://gitlab.com/gitlab-org/container-registry/commit/e74041697ccfb178749bd1b89395c2f07b2aee02))

# [3.11.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.10.1-gitlab...v3.11.0-gitlab) (2021-09-10)


### Bug Fixes

* **handlers:** use 400 Bad Request status for canceled requests ([30428c6](https://gitlab.com/gitlab-org/container-registry/commit/30428c69c3670ed5102e4692abf51b98ae2cf6c2))
* **log:** use same logger key between both logging packages ([04e2f68](https://gitlab.com/gitlab-org/container-registry/commit/04e2f68a6796222103da8a43eb6dfd06440f24cb))
* **storage:** provide detailed error when blob enumeration cannot parse digest from path ([f8d9d40](https://gitlab.com/gitlab-org/container-registry/commit/f8d9d40a5f664dc39c4b3182c5de601df6d17897))


### Features

* use ISO 8601 with millisecond precision as timestamp format ([2c56935](https://gitlab.com/gitlab-org/container-registry/commit/2c56935a69b04d21abc32dc6352ff7cb08e7b8c6))
* **handlers:** log metadata when a blob is downloaded ([ca37bff](https://gitlab.com/gitlab-org/container-registry/commit/ca37bffbe0d6803eb2c4f625375638bbf2df4fc0))
* **handlers:** use structured logging throughout ([97975de](https://gitlab.com/gitlab-org/container-registry/commit/97975dee9197f7e6ba078ca8cb5b64cbcd44fc7b))
* configurable expiry delay for storage backend presigned URLs ([820052a](https://gitlab.com/gitlab-org/container-registry/commit/820052a2ce9eed84746e80802c8fee37f2019394))

## [3.10.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.10.0-gitlab...v3.10.1-gitlab) (2021-09-03)


### Bug Fixes

* set prepared statements option for the CLI DB client ([1cc1716](https://gitlab.com/gitlab-org/container-registry/commit/1cc1716f8eb92b55cc462f23e32ff3f346ee6575))
* **configuration:** require rootdirectory to be set when in migration mode ([6701c98](https://gitlab.com/gitlab-org/container-registry/commit/6701c98e71a1bff26f30492f4965fc8d91754163))
* **gc:** improve handling of database errors and review postponing ([e359925](https://gitlab.com/gitlab-org/container-registry/commit/e3599255528f29f0a0cc8d75c6ee0b16122fbce2))

# [3.10.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.9.0-gitlab...v3.10.0-gitlab) (2021-08-23)


### Features

* **handlers:** log metadata when a manifest is uploaded or downloaded ([e078c9f](https://gitlab.com/gitlab-org/container-registry/commit/e078c9f5b2440193157c04ddbd101b5e04fddd32))
* **handlers:** log warning when eligibility flag is not set in migration mode ([fe78327](https://gitlab.com/gitlab-org/container-registry/commit/fe78327bc212b8089b2a8479eb458ec5a111c747))

# [3.9.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.8.0-gitlab...v3.9.0-gitlab) (2021-08-18)


### Features

* **handlers:** enable migration mode to be paused ([9d43c34](https://gitlab.com/gitlab-org/container-registry/commit/9d43c34b6323533ae99e53de81286a392a5a9635))

# [3.8.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.7.0-gitlab...v3.8.0-gitlab) (2021-08-17)


### Bug Fixes

* **handlers:** deny pushes for manifest lists with blob references except manifest cache images ([79e854a](https://gitlab.com/gitlab-org/container-registry/commit/79e854aabd25278760eb17e9e5507b180cf89cf0))
* **handlers:** enable cross repository blob mounts without FS mirroring ([98fe521](https://gitlab.com/gitlab-org/container-registry/commit/98fe521be56204aaaf330294a4e9bb7ccb2fb875))
* **handlers:** handle blob not found errors when serving head requests ([2492b4e](https://gitlab.com/gitlab-org/container-registry/commit/2492b4e4f060adec5215f17c8bf65a196edbd73b))
* **storage:** never write blob links when FS mirroring is disabled ([0786b77](https://gitlab.com/gitlab-org/container-registry/commit/0786b77573ea94d9b206012d2395d395470dabae))


### Features

* **datastore:** allow removing a connection from the pool after being idle for a period of time ([0352cc3](https://gitlab.com/gitlab-org/container-registry/commit/0352cc3fba180f277a9203a33cd936dc13ffd976))
* **storage/driver/s3-aws:** add IRSA auth support ([de69331](https://gitlab.com/gitlab-org/container-registry/commit/de693316925e3327a3b2ddf990233e8db640d6f7))


### Performance Improvements

* **handlers:** lookup single blob link instead of looping over all ([46f1642](https://gitlab.com/gitlab-org/container-registry/commit/46f16420d2fdffd96c230db6e36ef17f9750e9d5))

# [3.7.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.6.2-gitlab...v3.7.0-gitlab) (2021-08-06)


### Bug Fixes

* **auth/token:** fix migration eligibility validation for read requests ([6576897](https://gitlab.com/gitlab-org/container-registry/commit/657689735237b769b95eeb5b91e466675578473d))
* **handlers:** handle Buildkit index as an OCI manifest when using the database ([556ab04](https://gitlab.com/gitlab-org/container-registry/commit/556ab04690b8962bfc2aa386fefb0bd3a3a12d06))
* use MR diff SHA in commitlint job ([f66bccb](https://gitlab.com/gitlab-org/container-registry/commit/f66bccb9ef2449833f3eaa7327a0519c2776f42b))
* **handlers:** default to the schema 2 parser instead of schema 1 for manifest uploads ([0c62ea4](https://gitlab.com/gitlab-org/container-registry/commit/0c62ea47f801716ec3ee62eb7a37af2fd4b73115))
* **handlers:** display error details when invalid media types are detected during a manifest push ([f750297](https://gitlab.com/gitlab-org/container-registry/commit/f750297224ecd1f7549af484ef5a380caaf8aef4))
* **handlers:** fallback to OCI media type for manifests with no payload media type ([854f3ad](https://gitlab.com/gitlab-org/container-registry/commit/854f3adc937cd089e04926c1a4212249a17de84b))
* **handlers:** migration_path label should be logged as string ([524a614](https://gitlab.com/gitlab-org/container-registry/commit/524a6141201f15ada63bcfeae5d74375e0d7f558))
* **handlers:** return 400 Bad Request when saving a manifest with unknown media types on the DB ([0a39980](https://gitlab.com/gitlab-org/container-registry/commit/0a39980a0a6d08d040487a025746f5bed8e9c7de))
* **storage/driver/azure:** give deeply nested errors more context ([3388e1d](https://gitlab.com/gitlab-org/container-registry/commit/3388e1dc8ce1ae122e1069fa546ef165c1c13c52))


### Features

* **storage:** instrument blob download size and redirect option ([f091ff9](https://gitlab.com/gitlab-org/container-registry/commit/f091ff99dd5b9cf51c52964174dcfbe65f93c43a))


### Performance Improvements

* **storage:** do not descend into hashstates directors during upload purge ([b46d563](https://gitlab.com/gitlab-org/container-registry/commit/b46d56383df4a4814c9f2ef9a5612db27fd66ae9))

## [3.6.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.6.1-gitlab...v3.6.2-gitlab) (2021-07-29)

### Bug Fixes

* **handlers:** always add the migration_path label to HTTP metrics ([b152880](https://gitlab.com/gitlab-org/container-registry/commit/b1528807556dee9c92541256dc6688ded1a4979e))
* **handlers:** reduce noise from client disconnected errors during uploads ([61478d7](https://gitlab.com/gitlab-org/container-registry/commit/61478d74aa512c16bf1ff74282e3a23ee86566ea))
* **handlers:** set correct config media type when saving manifest on DB ([00f2c95](https://gitlab.com/gitlab-org/container-registry/commit/00f2c95901d59f36d781e98feaa9e30a0912686f))
* **storage:** return ErrManifestEmpty when zero-lenth manifest content is encountered ([1ad342b](https://gitlab.com/gitlab-org/container-registry/commit/1ad342becb5f7e2f93e1600c9842e99b7efa474a))

### Performance Improvements

* **handlers:** only read config from storage if manifest does not exist in DB ([8851793](https://gitlab.com/gitlab-org/container-registry/commit/8851793f2b06b1a15e908c897c32cae8ac318b36))

### Build System

* upgrade aliyungo dependency ([2d44f17](https://gitlab.com/gitlab-org/container-registry/commit/2d44f176f396013a27ecccf6367d1aefe5ce11a2))
* upgrade aws-sdk-go dependency to 1.40.7 ([e48843c](https://gitlab.com/gitlab-org/container-registry/commit/e48843c6716ce0c2ba9bd1bc6f3a43bde40ee8ef))
* upgrade backoff/v4 dependency to 4.1.1 ([abcb620](https://gitlab.com/gitlab-org/container-registry/commit/abcb6205f55f44ab18c0c5953fd2d3bbdcf8b41d))
* upgrade clock dependency to 1.1.0 ([15c8463](https://gitlab.com/gitlab-org/container-registry/commit/15c8463a45fb9465d06e440a04c214b9ff1949e6))
* upgrade cobra dependency to 1.2.1 ([fed057e](https://gitlab.com/gitlab-org/container-registry/commit/fed057e3a713da4027ff13862f82d038207e3a29))
* upgrade docker/libtrust dependency ([16adbf0](https://gitlab.com/gitlab-org/container-registry/commit/16adbf06d575ffe7a062654cfa8a3e0676ca170c))
* upgrade go-metrics dependency to 0.0.1 ([3b4eae0](https://gitlab.com/gitlab-org/container-registry/commit/3b4eae06c28990c068869ec64b2401310c80a487))
* upgrade golang.org/x/time dependency ([15b708c](https://gitlab.com/gitlab-org/container-registry/commit/15b708c07fe427460530664edcc6f2c05fc18177))
* upgrade labkit dependency to 1.6.0 ([d56a536](https://gitlab.com/gitlab-org/container-registry/commit/d56a5364eaab13afda721512093475c65ab77a92))
* upgrade opencontainer image-spec dependency to 1.0.1 ([c750921](https://gitlab.com/gitlab-org/container-registry/commit/c7509218195bcafd7492c3af1ec077860eb0ba6b))
* upgrade pgconn dependency to 1.10.0 ([756cf1b](https://gitlab.com/gitlab-org/container-registry/commit/756cf1b7c3ed38893bc3d29d0b489e1d3d3b1cb8))
* upgrade pgx/v4 dependency to 4.13.0 ([b3ed0df](https://gitlab.com/gitlab-org/container-registry/commit/b3ed0df30bda917aee9adc4021d3601e0a2edf0d))
* upgrade sentry-go dependency to 0.11.0 ([b1ec39f](https://gitlab.com/gitlab-org/container-registry/commit/b1ec39f9769ce608032b3bcd82254a72615944c3))
* upgrade sql-migrate dependency ([d00429e](https://gitlab.com/gitlab-org/container-registry/commit/d00429e453faadef4d57fa3965506da82cdf612a))

## [v3.6.1-gitlab] - 2021-07-23
### Changed
- registry/storage: Upgrade the GCS SDK to v1.16.0

### Fixed
- registry/storage: Offline garbage collection will continue if it cannot find a manifest referenced by a manifest list.

## [v3.6.0-gitlab] - 2021-07-20
### Changed
- registry/api/v2: Return 400 - Bad Request when client closes the connection, rather than returning 500 - Internal Server Error
- registry/storage: Upgrade Amazon S3 SDK to v1.40.3

## [v3.5.2-gitlab] - 2021-07-13
### Fixed
- registry/api/v2: Attempting to read a config through the manifests endpoint will now return a not found error instead of an internal server error.

## [v3.5.1-gitlab] - 2021-07-09
### Removed
- configuration: Remove proxy configuration migration section
- registry: Remove ability to migrate to remote registry

### Fixed
- registry/storage: Offline garbage collection now appropriately handles docker buildx cache manifests

### Added
- registry/api/v2: Log a warning when encountering a manifest list with blob references

## [v3.5.0-gitlab] - 2021-06-10
### Changed
- registry/datastore: Partitioning by top-level namespace

### Fixed
- registry/storage: Offline garbage collection no longer inappropriately removes untagged manifests referenced by a manifest list

### Added
- registry/storage: S3 Driver will now use Exponential backoff to retry failed requests

## [v3.4.1-gitlab] - 2021-05-11
### Fixed
- registry/storage: S3 driver now respects rate limits in all cases

### Changed
- registry/storage: Upgrade Amazon S3 SDK to v1.38.26
- registry/storage: Upgrade golang.org/x/time to v0.0.0-20210220033141-f8bda1e9f3ba
- registry: Upgrade github.com/opencontainers/go-digest to v1.0.0
- registry/storage: Upgrade Azure SDK to v54.1.0

## [v3.4.0-gitlab] - 2021-04-26
### Changed
- registry/datastore: Switch from 1 to 64 partitions per table

### Fixed
- registry: Log operating system quit signal as string

### Added
- registry/gc: Add Prometheus counter and histogram for online GC runs
- registry/gc: Add Prometheus counter and histogram for online GC deletions
- registry/gc: Add Prometheus counter for online GC deleted bytes
- registry/gc: Add Prometheus counter for online GC review postpones
- registry/gc: Add Prometheus histogram for sleep durations between online GC runs
- registry/gc: Add Prometheus gauge for the online GC review queues size

## [v3.3.0-gitlab] - 2021-04-09
### Added
- registry: Add Prometheus counter for database queries

### Changed
- registry/storage: Upgrade Azure SDK to v52.5.0

## [v3.2.1-gitlab] - 2021-03-17
### Fixed
- configuration: Don't require storage section for the database migrate CLI

## [v3.2.0-gitlab] - 2021-03-15
### Added
- configuration: Add `rootdirectory` option to the azure storage driver
- configuration: Add `trimlegacyrootprefix` option to the azure storage driver

## [v3.1.0-gitlab] - 2021-02-25
### Added
- configuration: Add `preparedstatements` option to toggle prepared statements for the metadata database
- configuration: Add `draintimeout` to database stanza to set optional connection close timeout on shutdown
- registry/api/v2: Disallow manifest delete if referenced by manifest lists (metadata database only).
- registry: Add CLI flag to facilitate programmatic state checks for database migrations
- registry: Add continuous online garbage collection

### Changed
- registry/datastore: Metadata database does not use prepared statements by default

## [v3.0.0-gitlab] - 2021-01-20
### Added
- registry: Experimental PostgreSQL metadata database (disabled by default)
- registry/storage/cache/redis: Add size and maxlifetime pool settings

### Changed
- registry/storage: Upgrade Swift client to v1.0.52

### Fixed
- registry/api: Fix tag delete response body

### Removed
- configuration: Drop support for TLS 1.0 and 1.1 and default to 1.2
- registry/storage/cache/redis: Remove maxidle and maxactive pool settings
- configuration: Drop support for logstash and combined log formats and default to json
- configuration: Drop support for log hooks
- configuration: Drop NewRelic reporting support
- configuration: Drop Bugsnag reporting support
- registry/api/v2: Drop support for schema 1 manifests and default to schema 2

## [v2.13.1-gitlab] - 2021-01-13
### Fixed
- registry: Fix HTTP request duration and byte size Prometheus metrics buckets

## [v2.13.0-gitlab] - 2020-12-15
### Added
- registry: Add support for a pprof monitoring server
- registry: Use GitLab LabKit for HTTP metrics collection
- registry: Expose build info through the Prometheus metrics

### Changed
- configuration: Improve error reporting when `storage.redirect` section is misconfigured
- registry/storage: Upgrade the GCS SDK to v1.12.0

### Fixed
- registry: Fix support for error reporting with Sentry

## [v2.12.0-gitlab] - 2020-11-23
### Deprecated
- configuration: Deprecate log hooks, to be removed by January 22nd, 2021
- configuration: Deprecate Bugsnag support, to be removed by January 22nd, 2021
- configuration: Deprecate NewRelic support, to be removed by January 22nd, 2021
- configuration: Deprecate logstash and combined log formats, to be removed by January 22nd, 2021
- registry/api: Deprecate Docker Schema v1 compatibility, to be removed by January 22nd, 2021
- configuration: Deprecate TLS 1.0 and TLS 1.1 support, to be removed by January 22nd, 2021

### Added
- registry: Add support for error reporting with Sentry
- registry/storage/cache/redis: Add Prometheus metrics for Redis cache store
- registry: Add TLS support for Redis
- registry: Add support for Redis Sentinel
- registry: Enable toggling redirects to storage backends on a per-repository basis

### Changed
- configuration: Cloudfront middleware `ipfilteredby` setting is now optional

### Fixed
- registry/storage: Swift path generation now generates multiple directories as intended
- registry/client/auth: OAuth token authentication now returns a `ErrNoToken` if a token is not found in the response
- registry/storage: Fix custom User-Agent header on S3 requests
- registry/api/v2: Text-charset selector removed from `application/json` content-type

## [v2.11.0-gitlab] - 2020-09-08
## Added
- registry: Add new configuration for changing the output for logs and the access logs format

## Changed
- registry: Use GitLab LabKit for correlation and logging
- registry: Normalize log messages

## [v2.10.0-gitlab] - 2020-08-05
## Added
- registry: Add support for continuous profiling with Google Stackdriver

## [v2.9.1-gitlab] - 2020-05-05
## Added
- registry/api/v2: Show version and supported extra features in custom headers

## Changed
- registry/handlers: Encapsulate the value of err.detail in logs in a JSON object

### Fixed
- registry/storage: Fix panic during uploads purge

## [v2.9.0-gitlab] - 2020-04-07
### Added
- notifications: Notification related Prometheus metrics
- registry: Make minimum TLS version user configurable
- registry/storage: Support BYOK for OSS storage driver

### Changed
- Upgrade to Go 1.13
- Switch to Go Modules for dependency management
- registry/handlers: Log authorized username in push/pull requests

### Fixed
- configuration: Fix pointer initialization in configuration parser
- registry/handlers: Process Accept header MIME types in case-insensitive way

## [v2.8.2-gitlab] - 2020-03-13
### Changed
- registry/storage: Improve performance of the garbage collector for GCS
- registry/storage: Gracefully handle missing tags folder during garbage collection
- registry/storage: Cache repository tags during the garbage collection mark phase
- registry/storage: Upgrade the GCS SDK to v1.2.1
- registry/storage: Provide an estimate of how much storage will be removed on garbage collection
- registry/storage: Make the S3 driver log level configurable
- registry/api/v2: Return not found error when getting a manifest by tag with a broken link

### Fixed
- registry/storage: Fix PathNotFoundError not being ignored in repository enumeration during garbage collection when WalkParallel is enabled

## v2.8.1-gitlab

- registry/storage: Improve consistency of garbage collection logs

## v2.8.0-gitlab

- registry/api/v2: Add tag delete route

## v2.7.8-gitlab

- registry/storage: Improve performance of the garbage collection algorithm for S3

## v2.7.7-gitlab

- registry/storage: Handle bad link files gracefully during garbage collection
- registry/storage: AWS SDK v1.26.3 update
- registry: Include build info on Prometheus metrics

## v2.7.6-gitlab

- CI: Add integration tests for the S3 driver
- registry/storage: Add compatibilty for S3v1 ListObjects key counts

## v2.7.5-gitlab

- registry/storage: Log a message if PutContent is called with 0 bytes

## v2.7.4-gitlab

- registry/storage: Fix Google Cloud Storage client authorization with non-default credentials
- registry/storage: Fix error handling of GCS Delete() call when object does not exist

## v2.7.3-gitlab

- registry/storage: Update to Google SDK v0.47.0 and latest storage driver (v1.1.1)

## v2.7.2-gitlab

- registry/storage: Use MD5 checksums in the registry's Google storage driver
