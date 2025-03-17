## [4.18.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.17.1-gitlab...v4.18.0-gitlab) (2025-03-17)

### ✨ Features ✨

* **api:** log tag delete events ([045dc26](https://gitlab.com/gitlab-org/container-registry/commit/045dc26a15089af4f3af20adab7e97af0fd565b5))
* **datastore:** support dedicated redis connection for DB load balancing ([98fa3f0](https://gitlab.com/gitlab-org/container-registry/commit/98fa3f0d6e807648ca25d4ba2d0b0f7a818a3812))

### 🐛 Bug Fixes 🐛

* cancel existing multipart uploads when starting new one for the same path for s3_v2 driver ([44a0d98](https://gitlab.com/gitlab-org/container-registry/commit/44a0d98c48b721acc370d302c3d3f34c5080b7d1))

### ⚙️ Build ⚙️

* **deps:** update module github.com/prometheus/client_golang to v1.21.1 ([801010a](https://gitlab.com/gitlab-org/container-registry/commit/801010a290771310ae3c606cc610b9202b9ebc69))
* **deps:** update module golang.org/x/crypto to v0.36.0 ([eb38c8c](https://gitlab.com/gitlab-org/container-registry/commit/eb38c8c7df01e6a1eda5077fbe4ecfbbfcbd8328))
* **deps:** update module golang.org/x/oauth2 to v0.28.0 ([33c43e9](https://gitlab.com/gitlab-org/container-registry/commit/33c43e97b73c9ca322cedd1e3aa9af9d83f04e1d))

## [4.17.1](https://gitlab.com/gitlab-org/container-registry/compare/v4.17.0-gitlab...v4.17.1-gitlab) (2025-03-06)

### 🐛 Bug Fixes 🐛

* **catalog:** list repositories with no unique layers ([5e6d53e](https://gitlab.com/gitlab-org/container-registry/commit/5e6d53eb5621e98f169fd929afc9dd1ded7b7441))
* fix bug in partial matching of a path in s3 driver's Stat call. ([acbf5e9](https://gitlab.com/gitlab-org/container-registry/commit/acbf5e9e380c0f0df6a977050cee655c75c780bd))

### ⚙️ Build ⚙️

* **deps:** add modified copy of redis_rate ([020b136](https://gitlab.com/gitlab-org/container-registry/commit/020b136935aef10a8f4a7c764530f9566b437a07))
* **deps:** update module github.com/opencontainers/image-spec to v1.1.1 ([a03242f](https://gitlab.com/gitlab-org/container-registry/commit/a03242f0de746601ba77865322006d055d9f538b))
* **deps:** update module gitlab.com/gitlab-org/api/client-go to v0.124.0 ([841e634](https://gitlab.com/gitlab-org/container-registry/commit/841e63428e00094bf358dbdfb3502e13091d7079))
* **deps:** update module gitlab.com/gitlab-org/labkit to v1.22.0 ([93af326](https://gitlab.com/gitlab-org/container-registry/commit/93af326d23fa9e0aebe15f0f8ceee1348d101336))

## [4.17.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.16.0-gitlab...v4.17.0-gitlab) (2025-02-28)

### ✨ Features ✨

* make azure v2 driver retries configurable ([4f3a71a](https://gitlab.com/gitlab-org/container-registry/commit/4f3a71adbaeef695c5af1bcce4ae9739bfd593b3))
* remove REGISTRY_FF_ENFORCE_LOCKFILES ([f81524e](https://gitlab.com/gitlab-org/container-registry/commit/f81524e87206449c474e5dce888181258f8e00c9))
* use SKIP_POST_DEPLOYMENT_MIGRATIONS env var to skip post-deployment migrations ([b81570d](https://gitlab.com/gitlab-org/container-registry/commit/b81570d24d08abe59b56f882e670796f464c87e9))

### 🐛 Bug Fixes 🐛

* allow only safe skip of post deploy migrations ([3a0275f](https://gitlab.com/gitlab-org/container-registry/commit/3a0275fe4e7c42b2b705c2ca8c44dbf33058eb4f))
* fix handling of Azure Blob Storage operation timeouts in azure_v2 driver ([06940d6](https://gitlab.com/gitlab-org/container-registry/commit/06940d679a40f4361bf1ccdbf7226dd8e7e82842))
* fix race issues in s3 retry code ([529df00](https://gitlab.com/gitlab-org/container-registry/commit/529df00a00daa8abe816f81078bda3a94b5f925b))
* **gcs:** do not wrap nil error when canceling writer ([9e578ef](https://gitlab.com/gitlab-org/container-registry/commit/9e578efa85b855cac4d6de586586f02515a4235c))
* permit logging of some of the headers by azure v2 driver ([335fc1a](https://gitlab.com/gitlab-org/container-registry/commit/335fc1a5bfe8e2d5e0c4926878f75a2784902ae0))

### ⏮️️ Reverts ⏮️️

* remove and enable REGISTRY_FF_ENFORCE_LOCKFILES by default ([3586c13](https://gitlab.com/gitlab-org/container-registry/commit/3586c137e4ad81a1640eab0233906207a2c9f875))

### ⚙️ Build ⚙️

* **deps:** update module github.com/azure/azure-sdk-for-go/sdk/azidentity to v1.8.2 ([4397052](https://gitlab.com/gitlab-org/container-registry/commit/4397052f36aba1affeba3d138d2b4f53036e4365))
* **deps:** update module github.com/prometheus/client_golang to v1.21.0 ([4071a44](https://gitlab.com/gitlab-org/container-registry/commit/4071a4448cc943a510cb0aa49cc210f42f8b4b02))
* **deps:** update module github.com/redis/go-redis/v9 to v9.7.1 ([cd6cd8d](https://gitlab.com/gitlab-org/container-registry/commit/cd6cd8dc199be5f60471a63ad00039c2920e42fe))
* **deps:** update module github.com/spf13/cobra to v1.9.1 ([8d282d7](https://gitlab.com/gitlab-org/container-registry/commit/8d282d7bee03cf262d07afde241aa10db9f1695f))
* **deps:** update module gitlab.com/gitlab-org/api/client-go to v0.123.0 ([c829a02](https://gitlab.com/gitlab-org/container-registry/commit/c829a02b2a800bf7fb60f6de7477b607078cbcda))
* **deps:** update module golang.org/x/crypto to v0.34.0 ([8ad06f4](https://gitlab.com/gitlab-org/container-registry/commit/8ad06f4a6e353d47f9bbaac6f40f6336554a9b7d))
* **deps:** update module golang.org/x/crypto to v0.35.0 ([85af5ac](https://gitlab.com/gitlab-org/container-registry/commit/85af5ac24fd373a1678fbd3eee16cb360139accb))
* **deps:** update module golang.org/x/oauth2 to v0.27.0 ([742336b](https://gitlab.com/gitlab-org/container-registry/commit/742336b266633c280bab70a2be3a92294cc203d7))

## [4.16.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.15.2-gitlab...v4.16.0-gitlab) (2025-02-13)


### ✨ Features ✨

* **datastore:** add count query for bbm status ([0880ee4](https://gitlab.com/gitlab-org/container-registry/commit/0880ee48d22503b4568613d02dd91ff77663f7b3))
* enable REGISTRY_FF_ENFORCE_LOCKFILES by default ([21a3d79](https://gitlab.com/gitlab-org/container-registry/commit/21a3d794de1f6f7df3752fdc3417d94465c76a72))
* tag immutability feature ([0525a74](https://gitlab.com/gitlab-org/container-registry/commit/0525a743bf66fbbcc8ab86ed6a4e7c1f3477b4a9))


### 🐛 Bug Fixes 🐛

* fix chunking in Azure v2 driver ([e3b5f1c](https://gitlab.com/gitlab-org/container-registry/commit/e3b5f1cf470d80d33d8b3aafec1ea93b4c8d9426))
* GCE storage driver was incorrectly reporting size of the blob ([e7c814a](https://gitlab.com/gitlab-org/container-registry/commit/e7c814ac2e6cfa511c0cebe512549eb2a034233a))
* rename new Azure v2 parameters to use snakecase ([3b3cdc8](https://gitlab.com/gitlab-org/container-registry/commit/3b3cdc806822b7a9310356a0a256fc262d69399b))


### ⚙️ Build ⚙️

* **deps:** update module github.com/azure/azure-sdk-for-go/sdk/storage/azblob to v1.6.0 ([9844159](https://gitlab.com/gitlab-org/container-registry/commit/9844159391a05cddb5907f3922ec7fd54a3104ad))
* **deps:** update module gitlab.com/gitlab-org/api/client-go to v0.121.0 ([dc8d551](https://gitlab.com/gitlab-org/container-registry/commit/dc8d5512961d8e0f7bd5a85f7d4cae1d6db01f19))
* **deps:** update module gitlab.com/gitlab-org/api/client-go to v0.122.0 ([0f68526](https://gitlab.com/gitlab-org/container-registry/commit/0f685262ca9302c63ce01995d4bee96441b9973f))
* **deps:** update module golang.org/x/crypto to v0.33.0 ([42155fb](https://gitlab.com/gitlab-org/container-registry/commit/42155fb0eaf0db1b18fe3c0f62bbf233b5eb36de))
* **deps:** update module golang.org/x/oauth2 to v0.26.0 ([d4725a5](https://gitlab.com/gitlab-org/container-registry/commit/d4725a5bdf7cfbde87711ddbb9abb44a6d96106b))
* **deps:** update module golang.org/x/sync to v0.11.0 ([0217da5](https://gitlab.com/gitlab-org/container-registry/commit/0217da58365b60ff5bbba209243b338c020fa40f))
* **deps:** update module golang.org/x/time to v0.10.0 ([05e4c00](https://gitlab.com/gitlab-org/container-registry/commit/05e4c0053f2f4d69c7b41f9dbb11ea5e7a06eee2))
* **deps:** update module google.golang.org/api to v0.218.0 ([7bb82ad](https://gitlab.com/gitlab-org/container-registry/commit/7bb82ad7e3626c8545fedcc1b632b66890924d68))
* **deps:** update module google.golang.org/api to v0.219.0 ([391bff9](https://gitlab.com/gitlab-org/container-registry/commit/391bff98a7435018635f544096e6f2f0df860727))

## [4.15.2](https://gitlab.com/gitlab-org/container-registry/compare/v4.15.1-gitlab...v4.15.2-gitlab) (2025-01-21)


### 🐛 Bug Fixes 🐛

* correctly assess parallelwalk config presence and value ([a5b0835](https://gitlab.com/gitlab-org/container-registry/commit/a5b0835f5c26c353f22ac0dab691944b22d3aafd))
* fix handling of unprefixed List() in gcs, add regression test ([ed37774](https://gitlab.com/gitlab-org/container-registry/commit/ed3777447f4fa1120d6256a8beb532315d2e569e))
* race condition while accessing dlb replicas ([21fce72](https://gitlab.com/gitlab-org/container-registry/commit/21fce72221d74237d04244a68872d0d2f96ba9f4))


### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.50.0 ([bf16e52](https://gitlab.com/gitlab-org/container-registry/commit/bf16e525e3d116035cc7e52c02927ec8fc134b0d))
* **deps:** update module github.com/azure/azure-sdk-for-go/sdk/azidentity to v1.8.1 ([eb57f54](https://gitlab.com/gitlab-org/container-registry/commit/eb57f5450ec50e6bc50a82521f84613bbcd39693))
* **deps:** update module gitlab.com/gitlab-org/api/client-go to v0.120.0 ([f5cf1ad](https://gitlab.com/gitlab-org/container-registry/commit/f5cf1ad1cf481e5012b4dcb992704ab4fe78ca84))
* **deps:** update module google.golang.org/api to v0.217.0 ([75129dd](https://gitlab.com/gitlab-org/container-registry/commit/75129dda2f05da0f316bf243e360efa921e17ad3))

## [4.15.1](https://gitlab.com/gitlab-org/container-registry/compare/v4.15.0-gitlab...v4.15.1-gitlab) (2025-01-14)


### 🐛 Bug Fixes 🐛

* add stack trace to a panic log ([bd70777](https://gitlab.com/gitlab-org/container-registry/commit/bd70777944c1d047dd8874ee94baea237fde5a54))

## [4.15.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.14.0-gitlab...v4.15.0-gitlab) (2025-01-14)


### ✨ Features ✨

* add single tag details API endpoint ([7158ad0](https://gitlab.com/gitlab-org/container-registry/commit/7158ad04ea1c10e4faf0d432827f43c273ea4833))
* add support for managed identies to Azure v2 storage driver ([ea370ee](https://gitlab.com/gitlab-org/container-registry/commit/ea370eef4fc1bb607b4d6716dc814949f9ef11f9))
* **bbm:** add initial bbm metrics ([4d831e8](https://gitlab.com/gitlab-org/container-registry/commit/4d831e8ee23a280712a371dcb014d1ee9432c630))
* **datastore:** detect read-only database and fail fast ([af1e2b6](https://gitlab.com/gitlab-org/container-registry/commit/af1e2b6796e8f029f4d466e0178ed2257a823dc8))
* implement azure driver using azblob module, name it v2 version ([65b6207](https://gitlab.com/gitlab-org/container-registry/commit/65b6207e3fa22e2ac1f56bce1109e11667e3428b))


### 🐛 Bug Fixes 🐛

* align Azure List() call internal pagination with that of S3 and GCS, enable testing it ([6990f3a](https://gitlab.com/gitlab-org/container-registry/commit/6990f3acfb0bca1895f9bf4f26c89b9fdd67f6e3))
* fix handling of overwrite of BlockBlob with AppendBlob in Azure storage driver, add regression tests ([1ae816b](https://gitlab.com/gitlab-org/container-registry/commit/1ae816b896469e46c2396ad4ccb2769c10471ba6))


### ⚙️ Build ⚙️

* **deps:** update dependency danger-review to v1.4.2 ([9d4d9b1](https://gitlab.com/gitlab-org/container-registry/commit/9d4d9b13440c1afece03230593eb3cd8b573edc3))
* **deps:** update dependency danger-review to v2 ([34ebe31](https://gitlab.com/gitlab-org/container-registry/commit/34ebe31013c2f6de3c1bbb60487b2e36114cde51))
* **deps:** update module cloud.google.com/go/storage to v1.48.0 ([de75ddf](https://gitlab.com/gitlab-org/container-registry/commit/de75ddf96c7d2053847d891263144a9cd7750501))
* **deps:** update module github.com/alicebob/miniredis/v2 to v2.34.0 ([bb0d1f7](https://gitlab.com/gitlab-org/container-registry/commit/bb0d1f771d33cad9eebb81f0d1a6e4242be4caea))
* **deps:** update module github.com/azure/azure-sdk-for-go/sdk/azcore to v1.17.0 ([4e62acc](https://gitlab.com/gitlab-org/container-registry/commit/4e62accda70eabcd621bafbea019b1c05a00380a))
* **deps:** update module github.com/eko/gocache/lib/v4 to v4.2.0 ([1d948d5](https://gitlab.com/gitlab-org/container-registry/commit/1d948d5edd0431fd54c590ad1a524571f8b7c833))
* **deps:** update module github.com/getsentry/sentry-go to v0.30.0 ([b6d3615](https://gitlab.com/gitlab-org/container-registry/commit/b6d3615fa7e002ca3726915f9f20933b7f4872c9))
* **deps:** update module github.com/getsentry/sentry-go to v0.31.1 ([5011cac](https://gitlab.com/gitlab-org/container-registry/commit/5011cac1186e4029a738af063fb4a27094047f47))
* **deps:** update module github.com/jackc/pgx/v5 to v5.7.2 ([bbb2a6e](https://gitlab.com/gitlab-org/container-registry/commit/bbb2a6e4b80fe4291839c6169d790457e3f82ac5))
* **deps:** update module github.com/rubenv/sql-migrate to v1.7.1 ([63cad34](https://gitlab.com/gitlab-org/container-registry/commit/63cad343187bf4b2d798e286af7036fd1b05da3d))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.18.0 ([8ce5d3b](https://gitlab.com/gitlab-org/container-registry/commit/8ce5d3b2c21ecf99848c1af37f28b1f7fd058e59))
* **deps:** update module gitlab.com/gitlab-org/api/client-go to v0.119.0 ([6aa1770](https://gitlab.com/gitlab-org/container-registry/commit/6aa1770096fb9f8aa46270a40990192c514d12bc))
* **deps:** update module golang.org/x/crypto to v0.30.0 ([6915029](https://gitlab.com/gitlab-org/container-registry/commit/6915029250548344096aaffeb213c2ae6bb62d83))
* **deps:** update module golang.org/x/crypto to v0.31.0 ([1e09441](https://gitlab.com/gitlab-org/container-registry/commit/1e094419072a005954248642dda97ebd021a7c87))
* **deps:** update module golang.org/x/crypto to v0.32.0 ([cec7530](https://gitlab.com/gitlab-org/container-registry/commit/cec7530bd8cebd1c2c6d53c5e3de6d28c56b6683))
* **deps:** update module golang.org/x/oauth2 to v0.25.0 ([563fd3b](https://gitlab.com/gitlab-org/container-registry/commit/563fd3ba9cc1f27cad83eaadb6ed8c4054d3f9bc))
* **deps:** update module golang.org/x/sync to v0.10.0 ([b14aa3b](https://gitlab.com/gitlab-org/container-registry/commit/b14aa3bc358c83068389fcec7de97e6e9e2b3084))
* **deps:** update module golang.org/x/time to v0.9.0 ([92c08dd](https://gitlab.com/gitlab-org/container-registry/commit/92c08dd3393e9d6553f679e17cec1c8eb7750e19))
* **deps:** update module google.golang.org/api to v0.210.0 ([1bd8827](https://gitlab.com/gitlab-org/container-registry/commit/1bd882732db4005a3b865d024da78755832d9622))
* **deps:** update module google.golang.org/api to v0.211.0 ([ae4ee16](https://gitlab.com/gitlab-org/container-registry/commit/ae4ee165b247cff7653fe85810656388f719bd70))
* **deps:** update module google.golang.org/api to v0.212.0 ([e63c433](https://gitlab.com/gitlab-org/container-registry/commit/e63c4334152ee034c03b5d1561c87c92dcd1e414))
* **deps:** update module google.golang.org/api to v0.213.0 ([c18d870](https://gitlab.com/gitlab-org/container-registry/commit/c18d87014ffd468386879fe6435ebb4411508196))
* **deps:** update module google.golang.org/api to v0.216.0 ([d3a999c](https://gitlab.com/gitlab-org/container-registry/commit/d3a999c7507a1ee66ed6ee2176ef248b72b5af2a))
* **deps:** use official go-gitlab package ([114ce56](https://gitlab.com/gitlab-org/container-registry/commit/114ce5634224d88221143498855fe085bdd1fb48))

## [4.14.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.13.0-gitlab...v4.14.0-gitlab) (2024-11-27)

### ✨ Features ✨

* add option to configure TLS ciphers ([38a3e89](https://gitlab.com/gitlab-org/container-registry/commit/38a3e89ad9b984630b7542ae17d918500ec53d73))
* **bbm:** add timing columns for bbm start and finish ([6ed457d](https://gitlab.com/gitlab-org/container-registry/commit/6ed457da6338c42c3e485c8970808c7124b7e617))
* change DB LB replica list update log entry fields ([ff6af11](https://gitlab.com/gitlab-org/container-registry/commit/ff6af11db5ff2bb5a1d003fc7d6a93210a069be2))
* implement tag protection feature ([e596e75](https://gitlab.com/gitlab-org/container-registry/commit/e596e75d029554735ebde35acba83e4660d2f04a))

### 🐛 Bug Fixes 🐛

* **bbm:** do not terminate run on transient failures ([0fa69cf](https://gitlab.com/gitlab-org/container-registry/commit/0fa69cfddb9b31af79d5843ccc6358c185183b53))
* **bbm:** record retrieval for ranges below batch size ([ac86e54](https://gitlab.com/gitlab-org/container-registry/commit/ac86e549722027c71d01c06fc8d07feb06a7b197))
* consolidate log key for DB replica address ([5edfe6c](https://gitlab.com/gitlab-org/container-registry/commit/5edfe6c55eedd12fb4610f9a957fc4f923c70143))
* **gc/worker:** ignore conn closed error ([e2b1390](https://gitlab.com/gitlab-org/container-registry/commit/e2b13909d4763694b1d10c58b5aaa032592a5e21))
* implement proper shutdown of container-registry healthchecks ([1522302](https://gitlab.com/gitlab-org/container-registry/commit/1522302a24fef77c0c41b50439a1bbd76b163890))
* make http check send a custom user agent ([0ac5855](https://gitlab.com/gitlab-org/container-registry/commit/0ac585512bdc214e5056b57e8fea74260b5cad99))
* minor security issues/bugs pointed out by gosec linter ([2636f56](https://gitlab.com/gitlab-org/container-registry/commit/2636f56715c3502f00b4ca292f78db459ae2e983))

### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.47.0 ([782eb9d](https://gitlab.com/gitlab-org/container-registry/commit/782eb9d2ec40e8d33378d0ebc61a31ba579ed29a))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.17.1 ([7a5ae49](https://gitlab.com/gitlab-org/container-registry/commit/7a5ae49aef600355fd96a6b53f092b6a5de35513))
* **deps:** update module github.com/stretchr/testify to v1.10.0 ([aa1ca72](https://gitlab.com/gitlab-org/container-registry/commit/aa1ca72ea9f1265af9fed5e43d2d613ecd198c56))
* **deps:** update module github.com/xanzy/go-gitlab to v0.113.0 ([6e88fbe](https://gitlab.com/gitlab-org/container-registry/commit/6e88fbed527cb285e045032b5afe5c19bcbfc48d))
* **deps:** update module github.com/xanzy/go-gitlab to v0.114.0 ([cc8343c](https://gitlab.com/gitlab-org/container-registry/commit/cc8343cd8dbb096f2e618cf4941d1d1fb0798c4f))
* **deps:** update module golang.org/x/crypto to v0.29.0 ([334149a](https://gitlab.com/gitlab-org/container-registry/commit/334149aeebd6d520ee3add4b40139ea81e86d28f))
* **deps:** update module golang.org/x/oauth2 to v0.24.0 ([b3c6a75](https://gitlab.com/gitlab-org/container-registry/commit/b3c6a75800f8831ba74682f0a0d128f3a9bebdfb))
* **deps:** update module golang.org/x/time to v0.8.0 ([4909774](https://gitlab.com/gitlab-org/container-registry/commit/49097748ba128278cd967179a4c3cdc9b08c3a39))
* **deps:** update module google.golang.org/api to v0.205.0 ([a99cae0](https://gitlab.com/gitlab-org/container-registry/commit/a99cae063f8b175d75d1e10820b8df37b21b9b0f))
* **deps:** update module google.golang.org/api to v0.206.0 ([3c02bc0](https://gitlab.com/gitlab-org/container-registry/commit/3c02bc0622c373a779f9374227c7e4dd0a69a27e))
* **deps:** update module google.golang.org/api to v0.207.0 ([8873aa0](https://gitlab.com/gitlab-org/container-registry/commit/8873aa0c09f8299bcfa89bcb92abad56d1c50282))
* **deps:** update module google.golang.org/api to v0.209.0 ([dd0af23](https://gitlab.com/gitlab-org/container-registry/commit/dd0af2396b236960675418c2b00335f2a6c3d88c))

## [4.13.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.12.0-gitlab...v4.13.0-gitlab) (2024-11-04)

### ✨ Features ✨

* **notifications:** add rename repository event notifier ([11950c1](https://gitlab.com/gitlab-org/container-registry/commit/11950c12c70de8fb4c479cbb365ee2cdf12e9459))

### 🐛 Bug Fixes 🐛

* **bbm:** ensure bbm query comply to pgx simple protocol implementation ([f097738](https://gitlab.com/gitlab-org/container-registry/commit/f09773871f6c01bf62105ea8c4fb6a7397d2eb1a))
* **bbm:** wait for shutdown signal asynchronously ([4ab392a](https://gitlab.com/gitlab-org/container-registry/commit/4ab392a76369e2bc316893bb8e177b992845d6ad))
* ensure DB LB replica resolution timeout does not prevent app startup ([6ff9db8](https://gitlab.com/gitlab-org/container-registry/commit/6ff9db8c2318305b1ca1afb5cc5ae6a985e9ab4f))

### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.46.0 ([c9bb58f](https://gitlab.com/gitlab-org/container-registry/commit/c9bb58fc4b7dc8c38f43e9bde700d24702b84129))
* **deps:** update module google.golang.org/api to v0.204.0 ([cbb92e5](https://gitlab.com/gitlab-org/container-registry/commit/cbb92e5ffe0a2a903d8230ff9447a3c7a69be436))

## [4.12.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.11.0-gitlab...v4.12.0-gitlab) (2024-10-29)

### ✨ Features ✨

* **api:** log tag override events ([0ffc35a](https://gitlab.com/gitlab-org/container-registry/commit/0ffc35ae727e490f021fd4337abd46b344d9542e))
* temporarily bump DB load balancing DNS timeout ([d3de2a3](https://gitlab.com/gitlab-org/container-registry/commit/d3de2a31a8245a48017b3bfcfb6738bc5efa9533))

### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.45.0 ([fc19527](https://gitlab.com/gitlab-org/container-registry/commit/fc1952719cddd0410826242438136ec0996040fb))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.17.0 ([54a31fa](https://gitlab.com/gitlab-org/container-registry/commit/54a31faa1a63f860e4c84b04d1d1f2b77f271e66))
* **deps:** update module go.uber.org/mock to v0.5.0 ([1b09600](https://gitlab.com/gitlab-org/container-registry/commit/1b096004d5aa4d093967df26dc703b9bfa075ff7))
* update module github.com/aws/aws-sdk-go to v1.55.5 ([b1565ab](https://gitlab.com/gitlab-org/container-registry/commit/b1565ab45bbbb7f45fcd7d18d8e4def1d3fde72c))
* update module google.golang.org/api to v0.203.0 ([9d990f8](https://gitlab.com/gitlab-org/container-registry/commit/9d990f8a97a1b35ebafd986c2b01492c2c84a7ee))

## [4.11.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.10.0-gitlab...v4.11.0-gitlab) (2024-10-21)

### ✨ Features ✨

* add filesystem lock file ([dbdd7ba](https://gitlab.com/gitlab-org/container-registry/commit/dbdd7bafebfd8eb2e69a006939ab034b46c5a997))
* add manifest ID to FK violation error message ([12f7b1a](https://gitlab.com/gitlab-org/container-registry/commit/12f7b1ad00f3e99defb9721e33d0f931216efd79))
* gracefully handle DLB replica resolve/connection failures ([14aeb62](https://gitlab.com/gitlab-org/container-registry/commit/14aeb62eb4c8f826682a5df56a88d25c4ac35bd6))

### 🐛 Bug Fixes 🐛

* ensure consistency of DLB primary LSN records ([42da9b1](https://gitlab.com/gitlab-org/container-registry/commit/42da9b13765b7860827216b66fedfb7785c5c14e))
* fix path traversal for inmemory storage ([f8951ec](https://gitlab.com/gitlab-org/container-registry/commit/f8951ec144bd2cab0295a9ba941830767d130814))

### ⚙️ Build ⚙️

* add gdk build dependencies for asdf ([8c93d76](https://gitlab.com/gitlab-org/container-registry/commit/8c93d76a95f90d95390ea80256d73e3a22552bd7))
* **deps:** update module cloud.google.com/go/storage to v1.44.0 ([637bdbb](https://gitlab.com/gitlab-org/container-registry/commit/637bdbbdcda058d5e6e5572f8262a4b91d9bf2d3))
* **deps:** update module github.com/getsentry/sentry-go to v0.29.1 ([df968f0](https://gitlab.com/gitlab-org/container-registry/commit/df968f058748d750a0c1605a228b497a3ea8a30e))
* **deps:** update module github.com/prometheus/client_golang to v1.20.5 ([6d4bd3f](https://gitlab.com/gitlab-org/container-registry/commit/6d4bd3f6311002cf5aa914bda982b2d5295a8a3e))
* **deps:** update module github.com/redis/go-redis/v9 to v9.6.2 ([d40ec8f](https://gitlab.com/gitlab-org/container-registry/commit/d40ec8f56a3dd1eca5b53a52f9bf9796c088289e))
* **deps:** update module github.com/redis/go-redis/v9 to v9.7.0 ([500f98b](https://gitlab.com/gitlab-org/container-registry/commit/500f98bcad306718df1018ea9c80915bf3b51c91))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.16.1 ([0405c6e](https://gitlab.com/gitlab-org/container-registry/commit/0405c6e494a8535eaefe24e586cdd84b55b5e8b8))
* **deps:** update module github.com/shopify/toxiproxy/v2 to v2.11.0 ([0b1621d](https://gitlab.com/gitlab-org/container-registry/commit/0b1621dea4ed56a2b4bdcab36ae9190ac503081b))
* **deps:** update module github.com/xanzy/go-gitlab to v0.110.0 ([156e38b](https://gitlab.com/gitlab-org/container-registry/commit/156e38bc153d20ea1bfb93aaa17740ea19f121c2))
* **deps:** update module github.com/xanzy/go-gitlab to v0.112.0 ([9448fc0](https://gitlab.com/gitlab-org/container-registry/commit/9448fc07ff8c9d04966940a39c712001fe5ceb1c))
* **deps:** update module gitlab.com/gitlab-org/labkit to v1.21.2 ([ec08f8d](https://gitlab.com/gitlab-org/container-registry/commit/ec08f8d9ebfac50e50e7aa1cc95c91f4630f594c))
* **deps:** update module golang.org/x/crypto to v0.28.0 ([37f68d6](https://gitlab.com/gitlab-org/container-registry/commit/37f68d612cf41ad4e73e2a67cd7677128b2eda09))
* **deps:** update module golang.org/x/time to v0.7.0 ([cdc3201](https://gitlab.com/gitlab-org/container-registry/commit/cdc3201a9c4526cc162c6ac4133142c6a028a327))
* **deps:** update module google.golang.org/api to v0.200.0 ([466e2ea](https://gitlab.com/gitlab-org/container-registry/commit/466e2ead363ada430233192ac9a78a6402995a10))
* **deps:** update module google.golang.org/api to v0.201.0 ([e83cb2a](https://gitlab.com/gitlab-org/container-registry/commit/e83cb2a0444ced5909cc5e466cd7c9913fd66e28))

## [4.10.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.9.0-gitlab...v4.10.0-gitlab) (2024-10-3)


### ✨ Features ✨

* **api:** enforce short timeout on DLB replica lookups ([92c08bb](https://gitlab.com/gitlab-org/container-registry/commit/92c08bbcfdcd03bcb593b02cff6d76997e0e5968))
* **bbm:** run background migration with db migrations on fresh install ([a56e6d1](https://gitlab.com/gitlab-org/container-registry/commit/a56e6d1015bde0d2cfcf374c36cb81c45d4823f0))
* **datastore:** close connections of retired replicas during DLB ([5963fef](https://gitlab.com/gitlab-org/container-registry/commit/5963fefaa5e84b47f5b06689449cd95cecc6babc))
* **db:** enforce bbm completion on schema migrations ([cceddd1](https://gitlab.com/gitlab-org/container-registry/commit/cceddd16d0d6aea1947f4a47138e00f097c8258e))
* record load balancing primary LSN on writes ([6c85928](https://gitlab.com/gitlab-org/container-registry/commit/6c85928577dcfc051491561e6632fc33beaf9295))
* remove digest package and binary ([abe0aff](https://gitlab.com/gitlab-org/container-registry/commit/abe0affda8f4435c0b375b94b7139677a5b95cda))


### 🐛 Bug Fixes 🐛

* eliminate race in http sink write during sink closing ([e6136da](https://gitlab.com/gitlab-org/container-registry/commit/e6136da7ce48f71086881379cd85e8b8a48584a3))
* **gc/worker:** prevent negative backoff durations ([b0263aa](https://gitlab.com/gitlab-org/container-registry/commit/b0263aac119e4f522fb3d253ddb8f84082380492))
* implement proper shutdown for App, fix goroutine leaks ([7b073c2](https://gitlab.com/gitlab-org/container-registry/commit/7b073c2872a1c06b31515b2c91577f5e7ab4d0cf))
* make backoffSink sink interruptable ([123b206](https://gitlab.com/gitlab-org/container-registry/commit/123b2068716f495e54e95f68b40627e13f8bb7e8))
* make broadcaster sink interruptable ([51cc5c4](https://gitlab.com/gitlab-org/container-registry/commit/51cc5c40429261af25fe849b0e523909dd83c102))
* make eventqueue sink interruptable ([4d0e463](https://gitlab.com/gitlab-org/container-registry/commit/4d0e4639cc15185401c8f14567fca4517464be67))
* make http sink interruptable ([d574ab7](https://gitlab.com/gitlab-org/container-registry/commit/d574ab752ad4d60e53ecb0eccda4dd8fa9557c67))
* make retryingSink sink interruptable ([ca0f7ba](https://gitlab.com/gitlab-org/container-registry/commit/ca0f7bafc941874a55e7014a6bb90d4a19ef6cd7))
* prevent DLB replica checking from delaying app start ([c09257f](https://gitlab.com/gitlab-org/container-registry/commit/c09257fffda2347e345b9b75de3f7cd227ff5865))
* proper propagation of logger to request context ([2fe3b82](https://gitlab.com/gitlab-org/container-registry/commit/2fe3b82fab164e9d086624b1642683d01373c41e))


### ⚡️ Performance Improvements ⚡️

* **redis:** clean up  redis cache use across codebase ([b463680](https://gitlab.com/gitlab-org/container-registry/commit/b463680207a14f2139dec826f4af3cbb2865c5bc))
* **storage/driver/s3-aws:** improve HTTP client parameters for s3 storage driver ([9a402de](https://gitlab.com/gitlab-org/container-registry/commit/9a402de4653ac93c71a3a33dad14ba5ac073baee))


### ⚙️ Build ⚙️

* **deps:** update module github.com/getsentry/sentry-go to v0.29.0 ([5f55645](https://gitlab.com/gitlab-org/container-registry/commit/5f55645bb8e021858b9d44a1cc79975468ab6680))
* **deps:** update module github.com/prometheus/client_golang to v1.20.4 ([72b84a9](https://gitlab.com/gitlab-org/container-registry/commit/72b84a9de6a20a24aa49a3945b2058a8a4781a72))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.15.0 ([fa07a25](https://gitlab.com/gitlab-org/container-registry/commit/fa07a25972450e86e622be973e52180ec5842c7e))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.16.0 ([ca76905](https://gitlab.com/gitlab-org/container-registry/commit/ca7690552083f538ee96abd38d89cb0b1f4c9411))
* **deps:** update module go.uber.org/automaxprocs to v1.6.0 ([0d2c370](https://gitlab.com/gitlab-org/container-registry/commit/0d2c370cc1e6a8a72b159a6ea662a6507837f8d6))
* **deps:** update module google.golang.org/api to v0.197.0 ([b1b8371](https://gitlab.com/gitlab-org/container-registry/commit/b1b83719769fa35ae33d4fe1ff9a3bd735fc40cb))
* **deps:** update module google.golang.org/api to v0.198.0 ([f0a706b](https://gitlab.com/gitlab-org/container-registry/commit/f0a706b0ee4184c45eb4c5204ee647df7c55d561))
* **deps:** update module google.golang.org/api to v0.199.0 ([6f82294](https://gitlab.com/gitlab-org/container-registry/commit/6f822947b565e64e088a5bda61e9235ae42f4bed))
* labkit downstream pipeline ([d2345c4](https://gitlab.com/gitlab-org/container-registry/commit/d2345c417a01d7705c42bbb6f8734492726fe0d6))

## [4.9.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.8.0-gitlab...v4.9.0-gitlab) (2024-09-11)


### ✨ Features ✨

* **bbm:** add background migration cli with status sub command ([fa618bb](https://gitlab.com/gitlab-org/container-registry/commit/fa618bbd6fd23c4e25cd5863ad108de561c4ddca))
* **bbm:** add bbm pause subcommand ([c31a7da](https://gitlab.com/gitlab-org/container-registry/commit/c31a7dae639d864c4b57fdc890453ef766b7eeb3))
* **bbm:** add resume command ([5da4879](https://gitlab.com/gitlab-org/container-registry/commit/5da4879ae6337e66f5b81fce90ce547d8bccbe3f))
* **bbm:** add run and resume commands for bbm cli ([703308a](https://gitlab.com/gitlab-org/container-registry/commit/703308a8a315821eb7a65ce4ae6ef1949199c7ff))
* **datastore:** check that connections to known DLB replicas are usable during a refresh ([c6ce2c4](https://gitlab.com/gitlab-org/container-registry/commit/c6ce2c4368eb10ae1e05d5f61e8f1719be2283c5))
* **datastore:** include DLB replica(s) stats in exported metrics ([cae49a9](https://gitlab.com/gitlab-org/container-registry/commit/cae49a971537bc736febd967ffccaef57d741ee9))
* **handlers:** use read-only replicas for calculating the size of repositories ([03284a4](https://gitlab.com/gitlab-org/container-registry/commit/03284a4750ae844ff051347c6e624d0ed462cca3))


### 🐛 Bug Fixes 🐛

* **datastore:** bypass DLB replica logic when appropriate ([1fc64dc](https://gitlab.com/gitlab-org/container-registry/commit/1fc64dcbcb50ceff8549b04411e48572b9b48487))
* **datastore:** do not override DLB replica DSNs in for loop ([5571a7f](https://gitlab.com/gitlab-org/container-registry/commit/5571a7f98ca1b6f4ba218b0b5fb9f3cca105ed9d))
* **datastore:** do not recycle existing replica connections ([98acfbe](https://gitlab.com/gitlab-org/container-registry/commit/98acfbef1cfdf93e71917e46ee68dd422e1cb5ba))
* make progress bar respect visibility settings ([abba238](https://gitlab.com/gitlab-org/container-registry/commit/abba238751678b26a9d20f33c2ff5f7f7605b3cc))


### ⏮️️ Reverts ⏮️️

* Revert "perf(redis):  Temporary revert of !1683" ([b6b3e5c](https://gitlab.com/gitlab-org/container-registry/commit/b6b3e5cffaf9a86dd7fbc2371a5550e4c579bc02))


### ⚙️ Build ⚙️

* **deps:** update module github.com/jackc/pgx/v5 to v5.7.1 ([a44a1fb](https://gitlab.com/gitlab-org/container-registry/commit/a44a1fb6762d06d3e71b92e459d2bc65e98209cc))
* **deps:** update module github.com/prometheus/client_golang to v1.20.0 ([cbf2899](https://gitlab.com/gitlab-org/container-registry/commit/cbf28991e8473888a141c595f779919c4eee41a3))
* **deps:** update module github.com/prometheus/client_golang to v1.20.2 ([eb3a3f0](https://gitlab.com/gitlab-org/container-registry/commit/eb3a3f022dd07393bb537ab709f7d75db422a8d5))
* **deps:** update module github.com/prometheus/client_golang to v1.20.3 ([d4bb30b](https://gitlab.com/gitlab-org/container-registry/commit/d4bb30bd9f0ee8bbd190d98168500863e2b5bac2))
* **deps:** update module github.com/xanzy/go-gitlab to v0.108.0 ([2610795](https://gitlab.com/gitlab-org/container-registry/commit/2610795fe979c13e20da6650f99b1a31f9bbd9c3))
* **deps:** update module github.com/xanzy/go-gitlab to v0.109.0 ([85c3478](https://gitlab.com/gitlab-org/container-registry/commit/85c347871678706b3abc854b09d2421f15c6b8f3))
* **deps:** update module golang.org/x/crypto to v0.27.0 ([26ddb37](https://gitlab.com/gitlab-org/container-registry/commit/26ddb37c8b35faa940815e52334a8bfc3d38a0a5))
* **deps:** update module golang.org/x/oauth2 to v0.23.0 ([39f5331](https://gitlab.com/gitlab-org/container-registry/commit/39f5331d506ab8cee788ffd6a36b38ac7bbc7e8e))
* **deps:** update module google.golang.org/api to v0.194.0 ([364a608](https://gitlab.com/gitlab-org/container-registry/commit/364a608a9b052ea89c79157e3611b7b75d0a71a7))
* **deps:** update module google.golang.org/api to v0.195.0 ([58d79a6](https://gitlab.com/gitlab-org/container-registry/commit/58d79a67f73c19dedb2aac6f7245c821c9ab7025))
* **deps:** update module google.golang.org/api to v0.196.0 ([bb13c23](https://gitlab.com/gitlab-org/container-registry/commit/bb13c2372f0efb9b0a2b688d35037fd6b8e5d4c2))

## [4.8.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.7.0-gitlab...v4.8.0-gitlab) (2024-8-22)


### ✨ Features ✨

* add Redis ACL username support in cache ([7e927fb](https://gitlab.com/gitlab-org/container-registry/commit/7e927fb3bc466ac354b40bdeab481d3884be055e))


### 🐛 Bug Fixes 🐛

* **api/gitlab/v1:** allow moving repository to its top-level-namespace ([ee7469d](https://gitlab.com/gitlab-org/container-registry/commit/ee7469d1864efba0c2abca9211107c6110bbf31d))


### ⏮️️ Reverts ⏮️️

* Revert "perf(redis): Temporary revert of !1679" ([55a2edd](https://gitlab.com/gitlab-org/container-registry/commit/55a2edd9bb3de46363567436bd5f6635d212a3a0))


### ⚙️ Build ⚙️

* **deps:** update module github.com/schollz/progressbar/v3 to v3.14.5 ([019aa9d](https://gitlab.com/gitlab-org/container-registry/commit/019aa9d12855e04292b74c2b8dde897c2225975d))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.14.6 ([bdaafef](https://gitlab.com/gitlab-org/container-registry/commit/bdaafef6fed5d066d9a5e70639cd364ce0964c3f))
* **deps:** update module golang.org/x/crypto to v0.26.0 ([d4bb81b](https://gitlab.com/gitlab-org/container-registry/commit/d4bb81bd575c217fcb17176b7c2b2ea5b06fed28))
* **deps:** update module golang.org/x/oauth2 to v0.22.0 ([225b489](https://gitlab.com/gitlab-org/container-registry/commit/225b4899f1a5270b5cfce041b7f2eeaf65a53688))
* **deps:** update module golang.org/x/time to v0.6.0 ([fe40709](https://gitlab.com/gitlab-org/container-registry/commit/fe4070918a13df9aae0a918ec718e14deae7b569))

## [4.7.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.6.0-gitlab...v4.7.0-gitlab) (2024-8-1)


### ✨ Features ✨

* add option to filter by exact string to the /gitlab/v1/../tags/list/ endpoint ([70b873a](https://gitlab.com/gitlab-org/container-registry/commit/70b873ade0d29415b43681cebdb828161d0ebefb))
* **cache:** use Redis repository cache for the check blob operation ([5df1e7c](https://gitlab.com/gitlab-org/container-registry/commit/5df1e7c7a55ec57a7f5a6c3f9fe0d94c506e574b))
* **datastore:** add database load balancing service discovery ([4e47073](https://gitlab.com/gitlab-org/container-registry/commit/4e47073e87a949eb63bf4dfa3ff663a399282189))
* **datastore:** introduce database load balancer entity ([b070146](https://gitlab.com/gitlab-org/container-registry/commit/b0701465b4f1fe478aa42d808db9c8e85c75190a))
* **datastore:** periodically refresh DB replica list during load balancing ([8dc9a5a](https://gitlab.com/gitlab-org/container-registry/commit/8dc9a5a93433cb55d5d6b1d5238d82bf6fe89856))
* **db:** add db queries for bbm ([2709a2c](https://gitlab.com/gitlab-org/container-registry/commit/2709a2cf5143a5aec194bd01e2515b41fd6d3597))
* **importer:** stop importing all repositories if tags table is not empty ([0e78c82](https://gitlab.com/gitlab-org/container-registry/commit/0e78c82a9d4b546cbd15e243dd1d09f0e8e4a783))
* **registry:** remove inventory tool ([d662c6a](https://gitlab.com/gitlab-org/container-registry/commit/d662c6a47515d46cdc50a525959a185c92e56f57))
* **registry:** remove require empty database option ([45ef380](https://gitlab.com/gitlab-org/container-registry/commit/45ef3803716a6db4c7575ccf47e953d42d5697ec))


### 🐛 Bug Fixes 🐛

* **handlers:** remove traces of v1/import route ([37636fa](https://gitlab.com/gitlab-org/container-registry/commit/37636fa065a346129a4b8abf93d1dd64d29d3b19))


### ⚡️ Performance Improvements ⚡️

* **redis:**  Temporary revert of !1683 ([9d179a1](https://gitlab.com/gitlab-org/container-registry/commit/9d179a1e440a3be34c20b88842087be96652f90e))
* **redis:** add Redis caching to get manifest endpoint ([96cffcc](https://gitlab.com/gitlab-org/container-registry/commit/96cffcc8854d4b18117749d83eb1f533e558b5c3))
* **redis:** add support for caching repository data in tags list API endpoint ([10fa925](https://gitlab.com/gitlab-org/container-registry/commit/10fa925b07bb34aecd7fc44fb3b146e72dbe57e7))
* **redis:** Temporary revert of !1679 ([ca00a18](https://gitlab.com/gitlab-org/container-registry/commit/ca00a18a5998c5893a8a2d3130d6ca9c3abb3830))


### ⚙️ Build ⚙️

* **deps:** switch from github.com/golang/mock to go.uber.org/mock ([cd46439](https://gitlab.com/gitlab-org/container-registry/commit/cd46439f232a08b8b207d981dcb5d15ca796f356))
* **deps:** update dependency danger-review to v1.4.1 ([44e71d3](https://gitlab.com/gitlab-org/container-registry/commit/44e71d329022b536c85cd3f69490408998086b4e))
* **deps:** update module cloud.google.com/go/storage to v1.43.0 ([82f22fc](https://gitlab.com/gitlab-org/container-registry/commit/82f22fce51bc98f6222abb0cffccdb7fc1b58d64))
* **deps:** update module github.com/alicebob/miniredis/v2 to v2.33.0 ([e8cef95](https://gitlab.com/gitlab-org/container-registry/commit/e8cef9502772294f53f2e48ccb23fd07b3d87004))
* **deps:** update module github.com/cenkalti/backoff/v4 to v4.3.0 ([467b9e5](https://gitlab.com/gitlab-org/container-registry/commit/467b9e564efc5ba623ece1b50051b84f26be6c67))
* **deps:** update module github.com/getsentry/sentry-go to v0.28.1 ([82b334a](https://gitlab.com/gitlab-org/container-registry/commit/82b334a38706785868b2cbd6ab12a6ed02c0eef5))
* **deps:** update module github.com/prometheus/client_golang to v1.19.1 ([d1c9170](https://gitlab.com/gitlab-org/container-registry/commit/d1c9170d2f93c691b3275dc9b3cbc128c53e772e))
* **deps:** update module github.com/redis/go-redis/v9 to v9.5.3 ([7504003](https://gitlab.com/gitlab-org/container-registry/commit/75040039b6ef2292efa6afb7bc0bb12e4e745004))
* **deps:** update module github.com/redis/go-redis/v9 to v9.5.4 ([25b1c01](https://gitlab.com/gitlab-org/container-registry/commit/25b1c01b9127aa8bf55de970d463cbb7d4bad04c))
* **deps:** update module github.com/redis/go-redis/v9 to v9.6.0 ([615cfbf](https://gitlab.com/gitlab-org/container-registry/commit/615cfbfd3d33868e84a213c0adcc06270ee71511))
* **deps:** update module github.com/redis/go-redis/v9 to v9.6.1 ([f6477e3](https://gitlab.com/gitlab-org/container-registry/commit/f6477e3689747e78b49e5cd7303c9a995e62afd5))
* **deps:** update module github.com/rubenv/sql-migrate to v1.7.0 ([c4ede5b](https://gitlab.com/gitlab-org/container-registry/commit/c4ede5bb47596ca4b7f48f60498a09bd91116246))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.14.4 ([2108236](https://gitlab.com/gitlab-org/container-registry/commit/2108236f9ceba855169c22854d37b0312ce7f696))
* **deps:** update module github.com/spf13/cobra to v1.8.1 ([d066f8d](https://gitlab.com/gitlab-org/container-registry/commit/d066f8d457c9cbfbdebaebe5cbd7dd72dfa27589))
* **deps:** update module github.com/spf13/viper to v1.19.0 ([71ae4b1](https://gitlab.com/gitlab-org/container-registry/commit/71ae4b13b8f1b3c9387325b2aa632934d33713ea))
* **deps:** update module github.com/xanzy/go-gitlab to v0.107.0 ([56552ed](https://gitlab.com/gitlab-org/container-registry/commit/56552ed5d7c7e9fa4b48d9446cfed95fc73b29a8))
* **deps:** update module golang.org/x/crypto to v0.25.0 ([1742972](https://gitlab.com/gitlab-org/container-registry/commit/1742972f64ad06f926ec5ef5e3a45bdb63b1d39e))
* **deps:** update module google.golang.org/api to v0.189.0 ([302327b](https://gitlab.com/gitlab-org/container-registry/commit/302327bc1986711094534e21096fd9aa51b6c1a3))

## [4.6.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.5.0-gitlab...v4.6.0-gitlab) (2024-06-28)


### ✨ Features ✨

* add configuration for database load balancing ([3ff52d2](https://gitlab.com/gitlab-org/container-registry/commit/3ff52d2447f427bc537f8313b86d89ecb40775cb))
* add new bbm config ([2b9c7ae](https://gitlab.com/gitlab-org/container-registry/commit/2b9c7ae1ee46f8cbeb60f0907d004c876afedcb4))
* **bbm:** add bbm first iteration ([d92cbca](https://gitlab.com/gitlab-org/container-registry/commit/d92cbca8b31bfb845b7636b3bce98b1a329c5eac))


### 🐛 Bug Fixes 🐛

* **importer:** skip unsupported digests errors ([94ae374](https://gitlab.com/gitlab-org/container-registry/commit/94ae374558769ae1d833a7541abd45ec26d4d5f5))


### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.42.0 ([f04936f](https://gitlab.com/gitlab-org/container-registry/commit/f04936f951536cbfc7f1080920e4347598b40468))
* **deps:** update module github.com/eko/gocache/lib/v4 to v4.1.6 ([ed7de4a](https://gitlab.com/gitlab-org/container-registry/commit/ed7de4a0d728d8bd87e616d145d8421b35acecc0))
* **deps:** update module github.com/jackc/pgx/v5 to v5.6.0 ([af182b4](https://gitlab.com/gitlab-org/container-registry/commit/af182b4d28fb1b559ba430d2cb0641e0d247a34c))
* **deps:** update module github.com/opencontainers/image-spec to v1.1.0 ([60ce22f](https://gitlab.com/gitlab-org/container-registry/commit/60ce22f6d5ffc8885db606efe9274b4633e25964))

## [4.5.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.4.0-gitlab...v4.5.0-gitlab) (2024-06-12)


### ✨ Features ✨

* bump root repository size cache TTL to 5m ([cd04225](https://gitlab.com/gitlab-org/container-registry/commit/cd04225dc3b2b4232cb826433ff4b1482d6e15de))
* **datastore:** create bbm tables ([d5b1834](https://gitlab.com/gitlab-org/container-registry/commit/d5b18344b1cfd18fb6bdbd5959cf6aff78e01052))

## [4.4.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.3.0-gitlab...v4.4.0-gitlab) (2024-6-10)


### ✨ Features ✨

* **api/gitlab/v1:** implement rename non top level namespace api ([7639d9a](https://gitlab.com/gitlab-org/container-registry/commit/7639d9af48af7e384cfb8f8884cc9261506a826c))
* **cache:** add support for Sentinel authentication ([ee2ce0d](https://gitlab.com/gitlab-org/container-registry/commit/ee2ce0db5afb1fa18ae4639cb89408687fc16704))
* **registry:** import-command: add step completion times to progress bar output ([ba74531](https://gitlab.com/gitlab-org/container-registry/commit/ba74531a34c3ce82f8d78abfbe13de7b3f91a546))
* **registry:** support dynamic media types ([0622f41](https://gitlab.com/gitlab-org/container-registry/commit/0622f4142370b77c85cf732bb0da103c707d17f2))

## [4.3.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.2.0-gitlab...v4.3.0-gitlab) (2024-06-04)


### ✨ Features ✨

* **api/gitlab/v1:** prevent early invalidation of top-level namespace cached sizes ([1fc2d08](https://gitlab.com/gitlab-org/container-registry/commit/1fc2d0813c93c4bf3aa1f0b476f4ef83504f75bc))


### 🐛 Bug Fixes 🐛

* **datastore:** add constraint to prevent inserts of empty media types ([b4ee29b](https://gitlab.com/gitlab-org/container-registry/commit/b4ee29bd3c9f15a5e490d4fac2bd42faa5864470))

## [4.2.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.1.0-gitlab...v4.2.0-gitlab) (2024-05-24)


### ✨ Features ✨

* **api/gitlab/v1:** log if repository size was obtained from cache ([8365e46](https://gitlab.com/gitlab-org/container-registry/commit/8365e467ffdd3af2ee03837ce7a83cf212cdddec))
* **gc:** allow gc to back off on intermittent failures ([c2a5be3](https://gitlab.com/gitlab-org/container-registry/commit/c2a5be36adef98904faf4451630fa9f9aab80b5f))


### 🐛 Bug Fixes 🐛

* **configuration:** fix json tag typo causing runtime panic ([f749e83](https://gitlab.com/gitlab-org/container-registry/commit/f749e83e109ff783e8cf9c3f0ecf4c798f8cef82))

## [4.1.0](https://gitlab.com/gitlab-org/container-registry/compare/v4.0.0-gitlab...v4.1.0-gitlab) (2024-05-06)


### ✨ Features ✨

* **api/gitlab/v1:** add jwt validation for moving project repositories ([a292b19](https://gitlab.com/gitlab-org/container-registry/commit/a292b190a988d92aa6b972ced67aa14788bc1c15))
* **api/gitlab/v1:** cache size with descendants of top-level namespaces ([9d1e730](https://gitlab.com/gitlab-org/container-registry/commit/9d1e7307f8e5756a1b7787bea84b3626ff10fe4c))

## [4.0.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.93.0-gitlab...v4.0.0-gitlab) (2024-4-29)


### ⚠ BREAKING CHANGES

* **storage:** remove OSS and Swift storage drivers
* **v2:** remove old tag delete api

### ✨ Features ✨

* **cache:** redis repository cache for list repository tags operation ([9ea6ebe](https://gitlab.com/gitlab-org/container-registry/commit/9ea6ebe2b4f6f0a7fdb85da6787e8d9b79bd6107))
* **storage:** remove OSS and Swift storage drivers ([32f4118](https://gitlab.com/gitlab-org/container-registry/commit/32f4118056300b9afaa755dd1faf35b9c9d356d2))
* **v2:** remove old tag delete api ([df7dbb0](https://gitlab.com/gitlab-org/container-registry/commit/df7dbb01d33ed705b0745153e42a6e8388e86112))

## [3.93.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.92.0-gitlab...v3.93.0-gitlab) (2024-04-23)


### ✨ Features ✨

* inject GitLab namespace and project IDs into CGS/CDN signed URLs ([ec9c72e](https://gitlab.com/gitlab-org/container-registry/commit/ec9c72e74bf8a1a78490c46abfe3f8f7188ac65d))
* **notifications:** add backoff sink with maxretries ([fa1a985](https://gitlab.com/gitlab-org/container-registry/commit/fa1a985737fcfe18b64943a1018c3535fcfc448e))
* **s3wrapper:** retry delete s3 objects operation if incomplete ([d911653](https://gitlab.com/gitlab-org/container-registry/commit/d91165374e8759bd0d05176ad705c3b8515c975a))

## [3.92.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.91.0-gitlab...v3.92.0-gitlab) (2024-04-04)


### ✨ Features ✨

* **api/gitlab/v1:** add last published timestamp to get repository details response ([801b7fc](https://gitlab.com/gitlab-org/container-registry/commit/801b7fc44fbc724f5eb4b17667beca508e494f61))


### 🐛 Bug Fixes 🐛

* **api/gitlab/v1:** 404 on v1 endpoint if database disabled ([6f5afcb](https://gitlab.com/gitlab-org/container-registry/commit/6f5afcbb4b055759c42d1137bb317e30142046aa))

## [3.91.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.90.0-gitlab...v3.91.0-gitlab) (2024-03-25)


### ✨ Features ✨

* add support for Timoni image and config media types ([131b5e4](https://gitlab.com/gitlab-org/container-registry/commit/131b5e41d496ca1fcdc6e7b0109c494416010276))
* **datastore:** add last_published_at timestamp to repositories table ([37437f0](https://gitlab.com/gitlab-org/container-registry/commit/37437f0c3f1dd0111b0f897ade2cac8cd88f731b))
* **handlers:** warn when online garbage collection is disabled ([59dd355](https://gitlab.com/gitlab-org/container-registry/commit/59dd355f177a841d86d173254607367f33db0830))


### 🐛 Bug Fixes 🐛

* allow OCI manifest subjects to reference non-OCI manifests ([5bae10a](https://gitlab.com/gitlab-org/container-registry/commit/5bae10a08ba5748bb03dc7fa2ded7f001618c04f))
* **v2:** oci conformance for blob upload ([3a4d00b](https://gitlab.com/gitlab-org/container-registry/commit/3a4d00be50c237ac62b056f63a038cebcba9e859))


### ⚙️ Build ⚙️

* **deps:** update github.com/jackc/pgerrcode digest to 6e2875d ([46f0e31](https://gitlab.com/gitlab-org/container-registry/commit/46f0e318877d8d885f8d4222bd870d1a2ba7f0a4))
* **deps:** update module cloud.google.com/go/storage to v1.39.0 ([5fa91c7](https://gitlab.com/gitlab-org/container-registry/commit/5fa91c7eae857a460cc793bce293bdc86e5c22c5))
* **deps:** update module cloud.google.com/go/storage to v1.39.1 ([81587b0](https://gitlab.com/gitlab-org/container-registry/commit/81587b01e992fc974887eed74ec7024ef0c15a91))
* **deps:** update module github.com/alicebob/miniredis/v2 to v2.32.1 ([40fd9e9](https://gitlab.com/gitlab-org/container-registry/commit/40fd9e932442f1ce24321a9d35956609d129c2b1))

## [3.90.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.89.0-gitlab...v3.90.0-gitlab) (2024-3-5)


### ✨ Features ✨

* add artifactType filtering to referrers in tags API ([2ef6710](https://gitlab.com/gitlab-org/container-registry/commit/2ef6710c380fb5bac9cd2768941b16a6c7f66719))


### ⚙️ Build ⚙️

* **deps:** update module github.com/prometheus/client_golang to v1.19.0 ([067b497](https://gitlab.com/gitlab-org/container-registry/commit/067b49744ae80bb87b7a29143599837a194fe3be))
* **deps:** update module github.com/shopify/toxiproxy/v2 to v2.8.0 ([edde091](https://gitlab.com/gitlab-org/container-registry/commit/edde0910d12b4b04cd8f6e35ba35e8cd381d0f4e))
* **deps:** update module github.com/xanzy/go-gitlab to v0.98.0 ([2f735b5](https://gitlab.com/gitlab-org/container-registry/commit/2f735b53a106d477f5a0d8863184ac429e5d39d8))
* **deps:** update module golang.org/x/crypto to v0.20.0 ([428015c](https://gitlab.com/gitlab-org/container-registry/commit/428015cd387a63a81d030a9de0195e6e8830e3ff))

## [3.89.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.88.1-gitlab...v3.89.0-gitlab) (2024-02-27)


### ✨ Features ✨

* add support for CUE module media types ([df220ac](https://gitlab.com/gitlab-org/container-registry/commit/df220ac7a55f4af2e8eb622ab3da3451a54972fb))
* Add support for empty media type from OCI spec version 1.1.0-rc4 documents ([17299df](https://gitlab.com/gitlab-org/container-registry/commit/17299dfa7e316fccd52f2d2af703667e8b46b59d))
* add support for Zarf Package media types ([3109e55](https://gitlab.com/gitlab-org/container-registry/commit/3109e558fe6ff47eb68c5733c39b2056f9526d61))
* **gcs:** add object size key to cdn and gcs signed url ([2876234](https://gitlab.com/gitlab-org/container-registry/commit/287623470958a67304164b85aeced6b246563f84))
* **gcs:** propagate request metadata to gcs audit logs ([8601478](https://gitlab.com/gitlab-org/container-registry/commit/86014783b43dde24521c6a885cd5d667b5802a9b))
* **googlecdn:** propagate request metadata to googlecdn audit logs ([39ea03d](https://gitlab.com/gitlab-org/container-registry/commit/39ea03d9dbe4803417e6858cb9288cac1e3e2aa4))


### 🐛 Bug Fixes 🐛

* **handlers:** do not write manifests on HEAD requests ([9bd696e](https://gitlab.com/gitlab-org/container-registry/commit/9bd696eeebe55906381a961ddee4bf140eafd019))


### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.37.0 ([8ca8d53](https://gitlab.com/gitlab-org/container-registry/commit/8ca8d535ebc8c71983efc308cd4694f2f5b56fbc))
* **deps:** update module cloud.google.com/go/storage to v1.38.0 ([b0d3114](https://gitlab.com/gitlab-org/container-registry/commit/b0d31146cd1ffe284ae341c671b6aba10452ceef))
* **deps:** update module github.com/getsentry/sentry-go to v0.27.0 ([26f36f5](https://gitlab.com/gitlab-org/container-registry/commit/26f36f545c8247cb83d1d0009a54939b2b1fa173))
* **deps:** update module github.com/jackc/pgx/v5 to v5.5.3 ([457f777](https://gitlab.com/gitlab-org/container-registry/commit/457f77731edd58599ebb7570fe801af5ae72eca0))
* **deps:** update module github.com/jszwec/csvutil to v1.10.0 ([0028224](https://gitlab.com/gitlab-org/container-registry/commit/0028224d941304c062cfc1f4cdbe9219d3f0ac4a))
* **deps:** update module github.com/redis/go-redis/v9 to v9.5.1 ([14a624c](https://gitlab.com/gitlab-org/container-registry/commit/14a624cd8d8475756c0aa5077989cd730181b44c))
* **deps:** update module github.com/schollz/progressbar/v3 to v3.14.2 ([2ae5931](https://gitlab.com/gitlab-org/container-registry/commit/2ae5931b76bf94bf7a68e35147916355c26f3133))
* **deps:** update module github.com/xanzy/go-gitlab to v0.97.0 ([a2401e0](https://gitlab.com/gitlab-org/container-registry/commit/a2401e0c7f448e08e89c1470db28b2564a4ff641))
* **deps:** update module golang.org/x/crypto to v0.19.0 ([8c452a5](https://gitlab.com/gitlab-org/container-registry/commit/8c452a54a844ea461043c41460a3eabf05ae9964))
* **deps:** update module golang.org/x/oauth2 to v0.17.0 ([3de9409](https://gitlab.com/gitlab-org/container-registry/commit/3de9409cdf5c3f5e62145411dc0d99277965ff8f))
* **deps:** update module google.golang.org/api to v0.162.0 ([51d011f](https://gitlab.com/gitlab-org/container-registry/commit/51d011fd6b8982516303ce369a4825a02e4f3d79))
* **deps:** update module google.golang.org/api to v0.167.0 ([1192583](https://gitlab.com/gitlab-org/container-registry/commit/1192583cce9e8d31bba538e6e48d7a9e22a3cfa5))

## [3.88.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.88.0-gitlab...v3.88.1-gitlab) (2024-01-30)


### 🐛 Bug Fixes 🐛

* **api/gitlab/v1:** fix timestamp validation in tag list api ([8ff0111](https://gitlab.com/gitlab-org/container-registry/commit/8ff0111a2ba2eaa96ba561f7ad91353311c37e26))
* **api:** add debug/vars endpoint to debug server ([9e56f81](https://gitlab.com/gitlab-org/container-registry/commit/9e56f81ee65bd7ab6d7c34bed8db5ae382c6374f))


### ⚡️ Performance Improvements ⚡️

* **db:** override database.pool.maxopen to 1 when applying migrations ([7371b7d](https://gitlab.com/gitlab-org/container-registry/commit/7371b7d335c86192d37d8995f542c11b663349fa))


### ⚙️ Build ⚙️

* **deps:** update module github.com/alicebob/miniredis/v2 to v2.31.1 ([92b1c35](https://gitlab.com/gitlab-org/container-registry/commit/92b1c35f59f0b525055a14255708b32937fec3c1))
* **deps:** update module github.com/data-dog/go-sqlmock to v1.5.2 ([241a6e0](https://gitlab.com/gitlab-org/container-registry/commit/241a6e03cb17659cc6798e3774ee8779718b8693))
* **deps:** update module github.com/getsentry/sentry-go to v0.26.0 ([8302ed8](https://gitlab.com/gitlab-org/container-registry/commit/8302ed80e8e20d63207ed21cf019defcc54c4f98))
* **deps:** update module github.com/jackc/pgx/v5 to v5.5.2 ([e130740](https://gitlab.com/gitlab-org/container-registry/commit/e1307409f975de3f2b8f5aa57c7e7273e0a92574))
* **deps:** update module github.com/prometheus/client_golang to v1.18.0 ([4fb2730](https://gitlab.com/gitlab-org/container-registry/commit/4fb2730356443b2d2d90b3a735453b19bd023dcb))
* **deps:** update module github.com/redis/go-redis/v9 to v9.3.1 ([3bb9ac2](https://gitlab.com/gitlab-org/container-registry/commit/3bb9ac241f3525fe4f14de8575c11e833443b555))
* **deps:** update module github.com/redis/go-redis/v9 to v9.4.0 ([90dbed5](https://gitlab.com/gitlab-org/container-registry/commit/90dbed5422f87c1b3fcc703d22f9fce75f3a5f8f))
* **deps:** update module github.com/spf13/viper to v1.18.2 ([e166c94](https://gitlab.com/gitlab-org/container-registry/commit/e166c944538e1f93727bd2742fd056f8d083eac7))
* **deps:** update module github.com/xanzy/go-gitlab to v0.96.0 ([eb859e0](https://gitlab.com/gitlab-org/container-registry/commit/eb859e0899c7f46b6aefb236ec602d563a0ef19f))
* **deps:** update module golang.org/x/crypto to v0.17.0 ([ed62613](https://gitlab.com/gitlab-org/container-registry/commit/ed626133ca468fc94ebd29d55e2d6fed03654640))
* **deps:** update module golang.org/x/crypto to v0.18.0 ([012d79f](https://gitlab.com/gitlab-org/container-registry/commit/012d79fd29741ef45284490f2ed7c37873ee8e9e))
* **deps:** update module golang.org/x/oauth2 to v0.16.0 ([05e6051](https://gitlab.com/gitlab-org/container-registry/commit/05e60510798d82c1947fe858e22f0bd16b78f003))
* **deps:** update module golang.org/x/sync to v0.6.0 ([956be7b](https://gitlab.com/gitlab-org/container-registry/commit/956be7b76daffa41fc271a988ccd7604d8df765b))
* **deps:** update module google.golang.org/api to v0.157.0 ([308508a](https://gitlab.com/gitlab-org/container-registry/commit/308508a0d01e06217c53882f7863a0e7e4cf8620))

## [3.88.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.87.0-gitlab...v3.88.0-gitlab) (2023-12-19)


### ✨ Features ✨

* add referrers data to internal List Tags API ([fa28ee6](https://gitlab.com/gitlab-org/container-registry/commit/fa28ee6d333f265fc80aeda127f61a3ceb6462df))
* **datastore:** importer: use progressbar to show import progress ([3be86b9](https://gitlab.com/gitlab-org/container-registry/commit/3be86b9622c635b5b446d763d08758923f991656))
* **importer:** import command: expose tag concurrency option gcs ([8a9fdcd](https://gitlab.com/gitlab-org/container-registry/commit/8a9fdcd044cdd4149dc258e9a0522f25bd10712d))


### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.36.0 ([117dad0](https://gitlab.com/gitlab-org/container-registry/commit/117dad048a2c467e8b89284e326974d24db1b483))
* **deps:** update module github.com/data-dog/go-sqlmock to v1.5.1 ([d7763ea](https://gitlab.com/gitlab-org/container-registry/commit/d7763ea0792fbd169bc5e6b0e8d352c319dcafcf))
* **deps:** update module github.com/jackc/pgx/v5 to v5.5.1 ([10b8550](https://gitlab.com/gitlab-org/container-registry/commit/10b85509deda2ff37a8cff78e500d87e7653bdf1))
* **deps:** update module github.com/jszwec/csvutil to v1.9.0 ([8e181b1](https://gitlab.com/gitlab-org/container-registry/commit/8e181b17501220a84c2c2e6287c724e151682231))
* **deps:** update module github.com/spf13/viper to v1.18.1 ([1bc5bca](https://gitlab.com/gitlab-org/container-registry/commit/1bc5bca7c74663417fa75f1276f1971792626127))
* **deps:** update module github.com/xanzy/go-gitlab to v0.95.1 ([2c10193](https://gitlab.com/gitlab-org/container-registry/commit/2c101935a9b936aec3f549899a0eea001525966a))
* **deps:** update module github.com/xanzy/go-gitlab to v0.95.2 ([7f1d8ec](https://gitlab.com/gitlab-org/container-registry/commit/7f1d8ec9e2a3c689ce0532bad61765d03ee09e4d))
* **deps:** update module google.golang.org/api to v0.153.0 ([1a9edc1](https://gitlab.com/gitlab-org/container-registry/commit/1a9edc11d4ae3ebb61de69c92d3a42b26a894cf7))
* **deps:** update module google.golang.org/api to v0.154.0 ([bf21ad7](https://gitlab.com/gitlab-org/container-registry/commit/bf21ad75336e11015f8bb37226197de44e05d95e))

## [3.87.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.86.2-gitlab...v3.87.0-gitlab) (2023-12-05)


### ✨ Features ✨

* **db:** add primary database config and use for migrations ([4d1bf37](https://gitlab.com/gitlab-org/container-registry/commit/4d1bf37ae35ff240feb1574db49b3a3d96fa50f3))
* enable registry to store artifactType with OCI manifests ([5e3b76f](https://gitlab.com/gitlab-org/container-registry/commit/5e3b76f5c5d80667d9b70e2ca5b125a4632f9ebd))
* **registry:** import-command: add debug-server option ([59064d9](https://gitlab.com/gitlab-org/container-registry/commit/59064d9814629693980c3f2dc3f774b049c5c739))


### 🐛 Bug Fixes 🐛

* **api/gitlab/v1:** order tag pagination correctly by publish_at ([3c29839](https://gitlab.com/gitlab-org/container-registry/commit/3c298398165d58d1974647b3dbf1e8651794d81f))
* don't skip check on post-deployment migrations prior to import ([6d76c0e](https://gitlab.com/gitlab-org/container-registry/commit/6d76c0e656af03150c5e75a74229078130190812))
* silence ominous maxprocs log message ([aaf8577](https://gitlab.com/gitlab-org/container-registry/commit/aaf857796b220c0f9665c8d39a7ebd57acd75d54))


### ⚙️ Build ⚙️

* **deps:** update module github.com/gorilla/mux to v1.8.1 ([a58bf89](https://gitlab.com/gitlab-org/container-registry/commit/a58bf899abbdfd2cc2a3bc4c2eae9aa32973a3dc))
* **deps:** update module github.com/spf13/cobra to v1.8.0 ([d85ac07](https://gitlab.com/gitlab-org/container-registry/commit/d85ac07c7f377e2ddec96e60b4e90f36d3423d38))
* **deps:** update module github.com/vmihailenco/msgpack/v5 to v5.4.1 ([4035764](https://gitlab.com/gitlab-org/container-registry/commit/4035764054ddcbfd50a45dda2cebb1ab4be10817))
* **deps:** update module github.com/xanzy/go-gitlab to v0.94.0 ([4d4b344](https://gitlab.com/gitlab-org/container-registry/commit/4d4b34404da1040a28e72c9eeba63c37da1390b6))
* **deps:** update module gitlab.com/gitlab-org/labkit to v1.21.0 ([51cc6a9](https://gitlab.com/gitlab-org/container-registry/commit/51cc6a9d62f1db4c4454d5402e64d772299992fe))
* **deps:** update module golang.org/x/crypto to v0.16.0 ([8eaf5e4](https://gitlab.com/gitlab-org/container-registry/commit/8eaf5e41e613a9bb2cba24cb1e65052169699b0d))
* **deps:** update module golang.org/x/oauth2 to v0.14.0 ([3089ffc](https://gitlab.com/gitlab-org/container-registry/commit/3089ffc180b1bae22a46033636099c31c9546773))
* **deps:** update module golang.org/x/oauth2 to v0.15.0 ([6f898f7](https://gitlab.com/gitlab-org/container-registry/commit/6f898f756a943d5dd40be52116778cc5ae1093d5))
* **deps:** update module golang.org/x/time to v0.4.0 ([1b23c8d](https://gitlab.com/gitlab-org/container-registry/commit/1b23c8dc0a9157ca1b399c2155da7cb65c64e381))
* **deps:** update module google.golang.org/api to v0.151.0 ([5a37085](https://gitlab.com/gitlab-org/container-registry/commit/5a37085cc9744c8bc1d717c0e8ebea2291fdbbf3))
* **deps:** update module google.golang.org/api to v0.152.0 ([a5ad8c7](https://gitlab.com/gitlab-org/container-registry/commit/a5ad8c708b9e9400f6463d1353a3727fe485f13c))

## [3.86.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.86.1-gitlab...v3.86.2-gitlab) (2023-11-14)


### 🐛 Bug Fixes 🐛

* **handlers:** race condition pushing manifest lists with the same digest ([57dcae0](https://gitlab.com/gitlab-org/container-registry/commit/57dcae059c6a80e572b8d96cd001b371d579d3c9))


### ⏮️️ Reverts ⏮️️

* **database:** use service discovery for primary address ([ce1b374](https://gitlab.com/gitlab-org/container-registry/commit/ce1b374ddcdbe05993a7cfa7e45fd9475ac50de9))


### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.35.1 ([49d315a](https://gitlab.com/gitlab-org/container-registry/commit/49d315a74db3720fc7e4c4c6251b7d5d5c3ff9a9))
* **deps:** update module github.com/gorilla/handlers to v1.5.2 ([6158b6b](https://gitlab.com/gitlab-org/container-registry/commit/6158b6b428360439a7f5e4741ebceac19b4de400))
* **deps:** update module github.com/redis/go-redis/v9 to v9.3.0 ([bab8af8](https://gitlab.com/gitlab-org/container-registry/commit/bab8af8314b4c37a073d57c234f34f2a4a9bac3d))

## [3.86.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.86.0-gitlab...v3.86.1-gitlab) (2023-11-08)


### ⚙️ Build ⚙️

* **deps:** update module github.com/spf13/viper to v1.17.0 ([9d1659f](https://gitlab.com/gitlab-org/container-registry/commit/9d1659f0a96f707138ea9bc775ed49dd3c345bda))
* **deps:** upgrade github.com/jackc/pgx driver to latest v5 ([d9769f8](https://gitlab.com/gitlab-org/container-registry/commit/d9769f8ad15ec15e1e107a703292282aacddb897))

## [3.86.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.85.0-gitlab...v3.86.0-gitlab) (2023-11-02)


### ✨ Features ✨

* add artifact_type_id to manifests table ([851773b](https://gitlab.com/gitlab-org/container-registry/commit/851773b57c20c907fa831bc7a2f628f4f62b8386))
* **cache:** use redis repository cache for manifest upload operation ([38ef156](https://gitlab.com/gitlab-org/container-registry/commit/38ef156ecdfb9a6f2d233e9fd2f6f2b3a3f7b973))
* **datastore:** gracefully import manifests with unknown layer media types ([2400331](https://gitlab.com/gitlab-org/container-registry/commit/24003317ea285a3ee5c9a4d6a18fdfda49033553))
* **storage:** deprecate OSS and Swift storage drivers ([f456ee0](https://gitlab.com/gitlab-org/container-registry/commit/f456ee00ff3cf226f96e5e86c225669e5e228cab))


### 🐛 Bug Fixes 🐛

* use correct name for manifests artifact ID FK constraint ([ab57e95](https://gitlab.com/gitlab-org/container-registry/commit/ab57e955fca4526c1b69d325b6d397be02dbbd1d))


### ⚡️ Performance Improvements ⚡️

* **db:** test connection to database fqdn ([e1f820b](https://gitlab.com/gitlab-org/container-registry/commit/e1f820b3baf09844552ebf5a0c61901d2254b528))


### ⚙️ Build ⚙️

* **deps:** update module cloud.google.com/go/storage to v1.33.0 ([e51df41](https://gitlab.com/gitlab-org/container-registry/commit/e51df41fae73b3f839952c415ea4cc27498d3157))
* **deps:** update module github.com/alicebob/miniredis/v2 to v2.31.0 ([909c7a9](https://gitlab.com/gitlab-org/container-registry/commit/909c7a9f5e99f54e0af9909a9f3e4d2e44230e6d))
* **deps:** update module github.com/aws/aws-sdk-go to v1.46.2 ([a5af822](https://gitlab.com/gitlab-org/container-registry/commit/a5af822c242728ea9a3fbd86ddae30ceae1fa014))
* **deps:** update module github.com/aws/aws-sdk-go to v1.46.7 ([75dcdd4](https://gitlab.com/gitlab-org/container-registry/commit/75dcdd453c2a8e5bc4adbc55c79f996c57760965))
* **deps:** update module github.com/eko/gocache/lib/v4 to v4.1.5 ([64e0218](https://gitlab.com/gitlab-org/container-registry/commit/64e02181d49caeb61ce99c7e25a039ceddea0c7e))
* **deps:** update module github.com/shopify/toxiproxy/v2 to v2.7.0 ([dc61a8c](https://gitlab.com/gitlab-org/container-registry/commit/dc61a8cbcf01657c9681f4170867439ed0ad4651))

## [3.85.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.84.0-gitlab...v3.85.0-gitlab) (2023-10-16)


### ✨ Features ✨

* add support for docker attestation media type ([3e32cd6](https://gitlab.com/gitlab-org/container-registry/commit/3e32cd6d6b4012e08c92f19ba3f764e3d3b01338))
* **api/gitlab/v1:** sort repository tags by published_at ([12de379](https://gitlab.com/gitlab-org/container-registry/commit/12de379f80aa5355a688c63796d5c64cdb01e07d))
* enable registry to store subject reference with OCI manifests ([bed5981](https://gitlab.com/gitlab-org/container-registry/commit/bed5981c0c53ecf1797ef7bc6642403cc648eee9))
* **handlers:** always use accurate layer media types ([7b85e5a](https://gitlab.com/gitlab-org/container-registry/commit/7b85e5a843dc2f205881a4da74b9ad274b0d1aef))
* **storage:** prevent offline gc from running against database metadata backed storage ([a224a21](https://gitlab.com/gitlab-org/container-registry/commit/a224a21515c5225e1a5d57813489b92a12d86d69))


### ⚙️ Build ⚙️

* **deps:** update module github.com/aws/aws-sdk-go to v1.45.14 ([08e35ce](https://gitlab.com/gitlab-org/container-registry/commit/08e35ce2ce09f78d32eaca53767bbd85bd069b6b))
* **deps:** update module github.com/aws/aws-sdk-go to v1.45.19 ([7d033e8](https://gitlab.com/gitlab-org/container-registry/commit/7d033e80b7031babde8e31b5cc37198ec08d4ef2))
* **deps:** update module github.com/aws/aws-sdk-go to v1.45.24 ([3e8f3c4](https://gitlab.com/gitlab-org/container-registry/commit/3e8f3c40d885cf961bb63868568093739846e3dd))
* **deps:** update module github.com/getsentry/sentry-go to v0.25.0 ([95f1348](https://gitlab.com/gitlab-org/container-registry/commit/95f1348c86ab13aca80f81e357bc355fff36841b))
* **deps:** update module github.com/prometheus/client_golang to v1.17.0 ([0dbe576](https://gitlab.com/gitlab-org/container-registry/commit/0dbe5763db4de340960609bb65c8354391a76fbc))
* **deps:** update module github.com/xanzy/go-gitlab to v0.92.1 ([5127f32](https://gitlab.com/gitlab-org/container-registry/commit/5127f32faf4575fa6c5dadb121efbc09533be3ad))
* **deps:** update module github.com/xanzy/go-gitlab to v0.92.3 ([06e937f](https://gitlab.com/gitlab-org/container-registry/commit/06e937fc3eb94cec7e810f945c3f06ad1bd54498))

## [3.84.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.83.0-gitlab...v3.84.0-gitlab) (2023-09-26)


### ✨ Features ✨

* **cache:** use repository cache to complete blob upload ([c678843](https://gitlab.com/gitlab-org/container-registry/commit/c67884383bcd88bf4a1c6083034b2bcac4dac81c))
* **handlers:** expose database usage in v2 headers ([121e440](https://gitlab.com/gitlab-org/container-registry/commit/121e440d086de331a4f4bd80d3b312b349a73412))
* **handlers:** use accurate layer media types by default ([1d7dc96](https://gitlab.com/gitlab-org/container-registry/commit/1d7dc9693ffc2cba08656cdf8485e7d9927b9743))
* use JSON format for sorting tags by name in descending order ([eb20a87](https://gitlab.com/gitlab-org/container-registry/commit/eb20a874c22e92f202205b77476d025d2e3f637a))


### ⚙️ Build ⚙️

* **deps:** update module github.com/aws/aws-sdk-go to v1.45.11 ([c2e742b](https://gitlab.com/gitlab-org/container-registry/commit/c2e742b76a9e026289cf8e99788a2b6b477819ac))
* **deps:** update module github.com/aws/aws-sdk-go to v1.45.6 ([97ac190](https://gitlab.com/gitlab-org/container-registry/commit/97ac1908b3c805955fa9492ff719cf6428cc3c3d))
* **deps:** update module github.com/getsentry/sentry-go to v0.24.0 ([3f90827](https://gitlab.com/gitlab-org/container-registry/commit/3f908279a7b943a94f0d2eb613ae675e67e1427f))
* **deps:** update module github.com/getsentry/sentry-go to v0.24.1 ([970f81a](https://gitlab.com/gitlab-org/container-registry/commit/970f81a482766ce34be0e48ffcf43a5dac98439e))
* **deps:** update module github.com/miekg/dns to v1.1.56 ([62c3c4e](https://gitlab.com/gitlab-org/container-registry/commit/62c3c4ebb98efd40801a255ccbfa11e695b45020))
* **deps:** update module golang.org/x/oauth2 to v0.12.0 ([837e746](https://gitlab.com/gitlab-org/container-registry/commit/837e7466583274a1462f9ef1b95d71859e8ea8ec))
* **deps:** update module google.golang.org/api to v0.142.0 ([2991e99](https://gitlab.com/gitlab-org/container-registry/commit/2991e993d4b449d5754f5c0aac41b10683f1fdd6))

## [3.83.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.82.2-gitlab...v3.83.0-gitlab) (2023-09-08)


### ✨ Features ✨

* use repository cache for start blob upload including blob mount ([70b62a9](https://gitlab.com/gitlab-org/container-registry/commit/70b62a97dd338d6d4996755f6adb5ee1c73a9c09))


### ⚙️ Build ⚙️

* **deps:** update module golang.org/x/crypto to v0.13.0 ([60a51bb](https://gitlab.com/gitlab-org/container-registry/commit/60a51bb0df00ed7feefc7d6be25b793c717e2e58))

## [3.82.2](https://gitlab.com/gitlab-org/container-registry/compare/v3.82.1-gitlab...v3.82.2-gitlab) (2023-09-06)


### 🐛 Bug Fixes 🐛

* **datastore:** drop repositories table unused migration columns ([09fafa5](https://gitlab.com/gitlab-org/container-registry/commit/09fafa592386e4b8581f6e6bf423a035c83e0f88))
* do not log unknown env var for feature flags ([12c905c](https://gitlab.com/gitlab-org/container-registry/commit/12c905c278e5e107010e1dbfce627881d24b135d))


### ⚙️ Build ⚙️

* **deps:** update module google.golang.org/api to v0.138.0 ([6000de4](https://gitlab.com/gitlab-org/container-registry/commit/6000de4d6237b34b73816f013495a2ad11a39ccb))

## [3.82.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.82.0-gitlab...v3.82.1-gitlab) (2023-09-05)


### 🐛 Bug Fixes 🐛

* **datastore:** drop repositories table unused migration columns ([4e7d5bb](https://gitlab.com/gitlab-org/container-registry/commit/4e7d5bb68f5eee32dc3262540d5ce99bb8f902e2))
* disable statement timeout for subject ID FK validation migrations ([7e8ecb3](https://gitlab.com/gitlab-org/container-registry/commit/7e8ecb39cc7a871450a6333afee411e085a3c542))


### ⏮️️ Reverts ⏮️️

* drop repositories table unused migration columns ([6576a7e](https://gitlab.com/gitlab-org/container-registry/commit/6576a7e10afecbab5479955c1aa767f9ddd23852))


### ⚙️ Build ⚙️

* **deps:** update module github.com/aws/aws-sdk-go to v1.45.2 ([33cd579](https://gitlab.com/gitlab-org/container-registry/commit/33cd579f913fb7dbf6cf9c79f9113d2591a900da))
* **deps:** update module github.com/xanzy/go-gitlab to v0.91.1 ([b3e045a](https://gitlab.com/gitlab-org/container-registry/commit/b3e045a2bf474e70c3fe41942cf194d0d052000d))
* **deps:** update module golang.org/x/oauth2 to v0.11.0 ([c586de0](https://gitlab.com/gitlab-org/container-registry/commit/c586de0d0669ad3751f5ae5707209a9002d674ea))

## [3.82.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.81.0-gitlab...v3.82.0-gitlab) (2023-09-01)


### ✨ Features ✨

* forward user JWT claims for webhook notifications ([715cb0c](https://gitlab.com/gitlab-org/container-registry/commit/715cb0c060349b9640f848003d43feb01b778f84))


### 🐛 Bug Fixes 🐛

* **handlers:** limit max v2/_catalog entries to 1000 ([cf635c3](https://gitlab.com/gitlab-org/container-registry/commit/cf635c335646013547aabc0a1e2270d01b2d8cde))


### ⚙️ Build ⚙️

* **deps:** update module github.com/azure/azure-sdk-for-go to v68.0.0 ([47df1ee](https://gitlab.com/gitlab-org/container-registry/commit/47df1ee68097871e92a703772a9cefda37965940))
* **deps:** update module gitlab.com/gitlab-org/labkit to v1.20.0 ([3815349](https://gitlab.com/gitlab-org/container-registry/commit/3815349ca0f07236d971971014f85c4e9c935cfb))
* **deps:** update module golang.org/x/crypto to v0.12.0 ([b897961](https://gitlab.com/gitlab-org/container-registry/commit/b89796111f3b7a81f9f33d60029ea353708f9863))
* upgrade github.com/vmihailenco/msgpack to v5 ([c9a0f3d](https://gitlab.com/gitlab-org/container-registry/commit/c9a0f3d2eee5a484fed026fc093923799adaf2c9))

## [3.81.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.80.0-gitlab...v3.81.0-gitlab) (2023-08-29)


### ✨ Features ✨

* add subject_id to manifests table ([8361276](https://gitlab.com/gitlab-org/container-registry/commit/836127676dae03ad8fda9c8c580d2efbfa7c39cc))
* **api/v2:** deprecate DELETE /v2/<name>/tags/reference/<tag> API endpoint ([fb43d7e](https://gitlab.com/gitlab-org/container-registry/commit/fb43d7ed7645f7827f0db5cdd3fddaee4a1d2c72))


### 🐛 Bug Fixes 🐛

* **v2:** handle content range header during layer chunk upload ([fae36b7](https://gitlab.com/gitlab-org/container-registry/commit/fae36b77c5d39775ba13558b9ae85376a78d0cca))


### ⚙️ Build ⚙️

* **deps:** update module github.com/alicebob/miniredis/v2 to v2.30.5 ([bad3cba](https://gitlab.com/gitlab-org/container-registry/commit/bad3cba18b776076a2c2a8a9f8c193fbe89fa5b5))
* **deps:** update module github.com/aws/aws-sdk-go to v1.44.323 ([c595854](https://gitlab.com/gitlab-org/container-registry/commit/c595854cedecf9a9cac2c2fdd697437fa56936be))
* **deps:** update module github.com/aws/aws-sdk-go to v1.44.327 ([b630712](https://gitlab.com/gitlab-org/container-registry/commit/b6307123d41a1114e9874724a91a1fc093bc1cc2))
* **deps:** update module github.com/aws/aws-sdk-go to v1.44.332 ([109c0ce](https://gitlab.com/gitlab-org/container-registry/commit/109c0cedb2806bd1a5fa1350aa1fa361e3636f5e))
* **deps:** update module github.com/eko/gocache/lib/v4 to v4.1.4 ([dde91b3](https://gitlab.com/gitlab-org/container-registry/commit/dde91b39d33de7aac73a21087cb96a8e0f7c1d36))
* **deps:** update module github.com/redis/go-redis/v9 to v9.1.0 ([60a4928](https://gitlab.com/gitlab-org/container-registry/commit/60a49283a53fc2c877044870bb99fa56444a5ba2))
* **deps:** update module github.com/shopify/toxiproxy to v2.5.0 ([5d54bb2](https://gitlab.com/gitlab-org/container-registry/commit/5d54bb2be6be3bc1e3acd827da706d9f0e431aee))
* **deps:** update module github.com/shopify/toxiproxy/v2 to v2.6.0 ([e9d67aa](https://gitlab.com/gitlab-org/container-registry/commit/e9d67aa90a6906f6fe42bd38a23e49085bd7defc))
* **deps:** update module github.com/spf13/cobra to v1.7.0 ([3dea471](https://gitlab.com/gitlab-org/container-registry/commit/3dea471257e651f3e4bd1a6f0940a3229567d68f))
* **deps:** update module github.com/spf13/viper to v1.16.0 ([825f022](https://gitlab.com/gitlab-org/container-registry/commit/825f02270d92039b10b36445f478cb31ab94f8d3))

## [3.80.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.79.0-gitlab...v3.80.0-gitlab) (2023-08-14)


### ✨ Features ✨

* add support for DELETE /v2/<name>/manifests/<tag> operation (OCI v1.1) ([a72ecc0](https://gitlab.com/gitlab-org/container-registry/commit/a72ecc086a8b5ab406c3d621b21e64cc1ede233b))
* add support for Development Containers media types ([352ab6b](https://gitlab.com/gitlab-org/container-registry/commit/352ab6b97203490069b0cca60e28209dc3046e65))
* add support for Falcoctl media types ([0694731](https://gitlab.com/gitlab-org/container-registry/commit/0694731c1e92b04eec6a44de27db2ef5ffe34223))
* **handlers:** add code path to check ongoing rename ([6ab5cff](https://gitlab.com/gitlab-org/container-registry/commit/6ab5cff997128bc16399bea676b89d98f0726cc5))


### 🐛 Bug Fixes 🐛

* **s3:** limit multi part upload max layer parts size ([8a32ccb](https://gitlab.com/gitlab-org/container-registry/commit/8a32ccbf4d15292556b3297cfedb4ac309902cba))


### ⚙️ Build ⚙️

* **deps:** update module github.com/aws/aws-sdk-go to v1.44.317 ([0be1318](https://gitlab.com/gitlab-org/container-registry/commit/0be1318f778c97b81bf1e16a2b71e7639417f9ee))
* **deps:** update module github.com/getsentry/sentry-go to v0.23.0 ([f3d1500](https://gitlab.com/gitlab-org/container-registry/commit/f3d15001429df1196c1bacd2a5653a52eba9ed11))
* **deps:** update module github.com/jackc/pgconn to v1.14.1 ([3d23ea3](https://gitlab.com/gitlab-org/container-registry/commit/3d23ea3d60475b69b2cee6a97752c715f9e71618))
* **deps:** update module github.com/jackc/pgx/v4 to v4.18.1 ([087933e](https://gitlab.com/gitlab-org/container-registry/commit/087933e488099524ba2981b9bf8bd4233f558432))
* **deps:** update module github.com/jszwec/csvutil to v1.8.0 ([427758d](https://gitlab.com/gitlab-org/container-registry/commit/427758d4bf484c49a72084c765b6697cdd7283d6))
* **deps:** update module github.com/prometheus/client_golang to v1.16.0 ([77e784c](https://gitlab.com/gitlab-org/container-registry/commit/77e784c00eb647d97b947229659c2a61a70878ae))
* **deps:** update module github.com/rubenv/sql-migrate to v1.5.2 ([57a9a9e](https://gitlab.com/gitlab-org/container-registry/commit/57a9a9ec2de2c872e1f7c1f7331ece1018b49af6))
* **deps:** update module github.com/stretchr/testify to v1.8.4 ([265c146](https://gitlab.com/gitlab-org/container-registry/commit/265c14672310bb27ad992fade6c47be16cc775fd))
* **deps:** update module go.uber.org/automaxprocs to v1.5.3 ([b2142e1](https://gitlab.com/gitlab-org/container-registry/commit/b2142e1e6a178e17adeba634a9757f5f9bb8318b))

## [3.79.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.78.0-gitlab...v3.79.0-gitlab) (2023-08-01)


### ✨ Features ✨

* add Cosign media types ([4387862](https://gitlab.com/gitlab-org/container-registry/commit/438786223e7ed15287adf63ed551cd654da23911))
* **api/gitlab/v1:** support sorting tags detail in descending order ([3c193e1](https://gitlab.com/gitlab-org/container-registry/commit/3c193e11c8c6fe5d56c1697b9528310d95f5eb54))
* **handlers:** use accurate media types for layers ([85f4d3a](https://gitlab.com/gitlab-org/container-registry/commit/85f4d3a330e2f98d022910758bfe642e1c360e21))
* **handlers:** use repository cache for the cross repository blob mount ([b875607](https://gitlab.com/gitlab-org/container-registry/commit/b875607c77b9db8b2664121b7bad1593d4c31ad2))


### 🐛 Bug Fixes 🐛

* **api/gitlab/v1:** do not set link header when it is empty ([24a3c97](https://gitlab.com/gitlab-org/container-registry/commit/24a3c976043af3d846cf392c11fc9314c495177b))
* **api/gitlab/v1:** rename fix and docs update ([f52942f](https://gitlab.com/gitlab-org/container-registry/commit/f52942fef465ec88299b724e32524c1e109229ee))
* **s3:** handle pagination of parts in s3 ([4871f7c](https://gitlab.com/gitlab-org/container-registry/commit/4871f7c57a9dfc97ed11dbe65277d561627c6ad0))


### ⚙️ Build ⚙️

* **deps:** update github.com/denverdino/aliyungo digest to ab98a91 ([e18857d](https://gitlab.com/gitlab-org/container-registry/commit/e18857dfce3ba16d4f00030b563731b78a4a7ae3))
* **deps:** update module github.com/alicebob/miniredis/v2 to v2.30.4 ([379ce81](https://gitlab.com/gitlab-org/container-registry/commit/379ce814e9ee5a5a671ac2a948a711bbb4b7486c))
* **deps:** update module github.com/aws/aws-sdk-go to v1.44.312 ([58381d9](https://gitlab.com/gitlab-org/container-registry/commit/58381d9eafe36f6b9c65eb6958b3e81a2c3edf00))
* **deps:** update module github.com/benbjohnson/clock to v1.3.5 ([aeb4be8](https://gitlab.com/gitlab-org/container-registry/commit/aeb4be85e577920a4c359e0df0c23b722a8373c6))
* **deps:** update module github.com/cenkalti/backoff/v4 to v4.2.1 ([f3f50d3](https://gitlab.com/gitlab-org/container-registry/commit/f3f50d3305683bd3f8a1783a08473b82f61d6bf3))
* **deps:** update module github.com/go-redis/redismock/v9 to v9.0.3 ([e04acb8](https://gitlab.com/gitlab-org/container-registry/commit/e04acb8f158ebe5c4eddf5d6c953c8d526c7018d))
* **deps:** update module github.com/redis/go-redis/v9 to v9.0.5 ([c24fffe](https://gitlab.com/gitlab-org/container-registry/commit/c24fffe2c514374d011f085ee19d4e015a1f7a33))
* upgrade cloud.google.com/go/storage from v1.29.0 to v1.31.0 ([cf94c33](https://gitlab.com/gitlab-org/container-registry/commit/cf94c33c478416706a614ea299b5ad8426f2d356))

# [3.78.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.77.0-gitlab...v3.78.0-gitlab) (2023-07-05)


### Bug Fixes

* upgrade github.com/miekg/dns to v1.1.15 ([8792ceb](https://gitlab.com/gitlab-org/container-registry/commit/8792ceb36d5da78f8428489888ca1d975a6b3412))


### Features

* **api/gitlab/v1:** support before query in repository tags list ([c3cda5e](https://gitlab.com/gitlab-org/container-registry/commit/c3cda5ee0ac385d52ab96fcfba4e9d49ee903ec4))
* **handlers:** use Redis repository cache for OCI blob delete operation ([a841762](https://gitlab.com/gitlab-org/container-registry/commit/a841762e1666d3514ddb7d3296c9eb8b688902c0))

# [3.77.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.76.0-gitlab...v3.77.0-gitlab) (2023-06-15)


### Features

* **api/gitlab/v1:** add project lease procurement to rename api ([3129e6e](https://gitlab.com/gitlab-org/container-registry/commit/3129e6e503f454d715539f85dc53387bbba079b2))
* **ff:** add OngoingRenameCheck feature flag ([1b6b74c](https://gitlab.com/gitlab-org/container-registry/commit/1b6b74c3c44138cb0801a23cb96d9de0fa60c1e5))
* **notification:** add blob download meta object to notifications ([c240863](https://gitlab.com/gitlab-org/container-registry/commit/c2408631b88d749922c2ae4df1687ea538fe920f))

# [3.76.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.75.0-gitlab...v3.76.0-gitlab) (2023-06-06)


### Features

* **api/gitlab/v1:** filter tags by name on List Repository Tags endpoint ([094db66](https://gitlab.com/gitlab-org/container-registry/commit/094db66344e47e06dd039cb3e3cb5ac856c9a244))
* **auth:** parse and log JWT meta project path ([0fed8be](https://gitlab.com/gitlab-org/container-registry/commit/0fed8be2cddb836e38fffe91138bf032c11fa973))

# [3.75.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.74.0-gitlab...v3.75.0-gitlab) (2023-05-24)


### Bug Fixes

* **handlers:** add debug messages for repository blob link checking ([42f6bba](https://gitlab.com/gitlab-org/container-registry/commit/42f6bba50c179134a5b9c87f4bad66ce44e0fc22))
* **storage:** prevent panic in inmemory driver ([6d2935d](https://gitlab.com/gitlab-org/container-registry/commit/6d2935da4c05fa33d981bfcbb51586c3f03648f4))


### Features

* **api/gitlab/v1:** revert filter tags by name on List Repository Tags endpoint ([1d79415](https://gitlab.com/gitlab-org/container-registry/commit/1d79415b01813c271109daf524efa7abdac13fde))
* **notification:** revert add blob download meta object to notifications ([1317de1](https://gitlab.com/gitlab-org/container-registry/commit/1317de1c3965d18a6e343d84201a868ac637a58a))

# [3.74.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.73.1-gitlab...v3.74.0-gitlab) (2023-05-18)


### Features

* **api/gitlab/v1:** filter tags by name on List Repository Tags endpoint ([e7c6630](https://gitlab.com/gitlab-org/container-registry/commit/e7c66304bd3a0fa555c3f107c5460a8ae52928ef))
* **notification:** add blob download meta object to notifications ([5d7c130](https://gitlab.com/gitlab-org/container-registry/commit/5d7c130287bab4789eb073c93a53303988729cbc))

## [3.73.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.73.0-gitlab...v3.73.1-gitlab) (2023-05-16)


### Bug Fixes

* **handlers:** disable filesystem layer link metadata if the database is enabled ([aae69ab](https://gitlab.com/gitlab-org/container-registry/commit/aae69ab826d412dd0785d1f5c0175afd4a1188b5))

# [3.73.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.72.0-gitlab...v3.73.0-gitlab) (2023-05-08)


### Features

* **handlers:** log unknown layer media types ([e53aee9](https://gitlab.com/gitlab-org/container-registry/commit/e53aee93c459ea7fb4c79b5fa293cc0b31237ba8))

# [3.72.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.71.0-gitlab...v3.72.0-gitlab) (2023-05-02)


### Bug Fixes

* **datastore:** importer: consistently store config blob media types in database ([4cf8c52](https://gitlab.com/gitlab-org/container-registry/commit/4cf8c52817c7740d368995567ba6f236bbc4bccb))
* **datastore:** importer: consistently store layer blob media types in database ([fc7ce4b](https://gitlab.com/gitlab-org/container-registry/commit/fc7ce4bc8b7a8246ede7d70af77759ad4c355bc4))
* **datastore:** importer: display counters for new import methods ([27cfc44](https://gitlab.com/gitlab-org/container-registry/commit/27cfc44344f7b1a09e5dc14a6525e4c7f05c936a))
* **gc:** ignore broken tag link ([db3d8d7](https://gitlab.com/gitlab-org/container-registry/commit/db3d8d75e8aedd9dd79146ac7aac501c049546c4))


### Features

* **api/gitlab/v1:** add config digest to List Repository Tags response ([299fa44](https://gitlab.com/gitlab-org/container-registry/commit/299fa44f8c27d679db6877ee66848f67f5658521))
* **azure:** introduce azure storage driver legacyrootprefix config ([4314deb](https://gitlab.com/gitlab-org/container-registry/commit/4314debc14e14b6ef0d30e4215445e25b6bda86c))
* **database:** use service discovery for primary address ([8fcbca0](https://gitlab.com/gitlab-org/container-registry/commit/8fcbca0e6ef8f829e2a405b266a38306eaf3feeb))
* **handlers:** remove online migration routes ([0fb586a](https://gitlab.com/gitlab-org/container-registry/commit/0fb586af957044739b03c741f763799a8176940b))
* **proxy:** remove pull-through proxy cache feature ([557061d](https://gitlab.com/gitlab-org/container-registry/commit/557061d481bba5d83cb5c80a171f89c95c20e3e4))
* remove migration path label from storage metrics ([f7c9402](https://gitlab.com/gitlab-org/container-registry/commit/f7c9402286e39ef52acdc50a341d953776e924fb))


### Performance Improvements

* add uber automaxprocs package ([873ae97](https://gitlab.com/gitlab-org/container-registry/commit/873ae97dec238bc4823a0ce135d878ee013fefcc))

# [3.71.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.70.0-gitlab...v3.71.0-gitlab) (2023-04-14)


### Features

* add support for Flux media types ([bf844d6](https://gitlab.com/gitlab-org/container-registry/commit/bf844d66fca9a4fdf2cd368209e4e3c74590ac8d))
* **api/gitlab/v1:** update rename endpoint to use redis ([2a6f01a](https://gitlab.com/gitlab-org/container-registry/commit/2a6f01ab43bf21093ce50c47a614797333ad855b))
* **notifications:** add endpoint name label to Prometheus metrics ([6fe8cc2](https://gitlab.com/gitlab-org/container-registry/commit/6fe8cc2b0dbb3e8708ffecbe85855b553c9739ea))

# [3.70.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.69.0-gitlab...v3.70.0-gitlab) (2023-03-31)


### Bug Fixes

* **cache:** check for any type of redisNil wrapped error ([574a7b1](https://gitlab.com/gitlab-org/container-registry/commit/574a7b12c5eec7ec608abb85d02152c59853ddbd))
* **registry:** import-command: error out when multiple import step options are provided ([981cbed](https://gitlab.com/gitlab-org/container-registry/commit/981cbedbaa039e11506371883399e30d73edcca2))
* **s3:** propagate the new objectOwnership param to S3 constructor ([e7e1658](https://gitlab.com/gitlab-org/container-registry/commit/e7e1658aa84df9376145127b8e87c8b5d8a1e814))


### Features

* **registry:** import-command: remove dangling manifest option ([a245b5b](https://gitlab.com/gitlab-org/container-registry/commit/a245b5bb5e1f668f18ae9fded5e10fa5b9e788a1))
* **registry:** import-command: remove single repository option ([435a46a](https://gitlab.com/gitlab-org/container-registry/commit/435a46ae0a42a2bd83e654940426f3df22c74b8d))
* **registry:** importer: remove blob transfer ([40285f9](https://gitlab.com/gitlab-org/container-registry/commit/40285f93f71701af3c9bb6f85d58b5a69cee2572))


### Performance Improvements

* **api:** improve query performance for listing repositories ([51d58e3](https://gitlab.com/gitlab-org/container-registry/commit/51d58e39ef3e9fdfa6bb82f9a3a6474087031f1c))

# [3.69.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.68.0-gitlab...v3.69.0-gitlab) (2023-03-16)


### Bug Fixes

* **datastore:** do not use a transaction for full pre import repositories ([d41f64a](https://gitlab.com/gitlab-org/container-registry/commit/d41f64aea14b1ca6c4b84fc22bd28ad80117e00b))
* **gc:** revert temporary debug log for deadlocks during manifest deletes ([b06a277](https://gitlab.com/gitlab-org/container-registry/commit/b06a27752fb543306363df018ce9d40a2a25e5cc))
* **handlers:** serve OCI index payloads with no media type field ([280fd79](https://gitlab.com/gitlab-org/container-registry/commit/280fd798c7b107216b9123164123fe879d2459c7))
* revert temporarily log query duration for debugging ([a184751](https://gitlab.com/gitlab-org/container-registry/commit/a184751f96b586d7bbabbee5dd917034f60f36d2))


### Features

* **api/gitlab/v1:** add rename repository endpoint ([365e740](https://gitlab.com/gitlab-org/container-registry/commit/365e740393176a58a2532ec6eaf7c5d5f2ac539c))
* **datastore:** add all-repositories option to import command ([4fb3682](https://gitlab.com/gitlab-org/container-registry/commit/4fb36825a18d4c297cc36beb25e84f2888a9bf86))
* **db:** add query to perform rename of repository ([92de2b9](https://gitlab.com/gitlab-org/container-registry/commit/92de2b983b5af1a3e14b6b210c4d577906fcea5c))
* **registry:** import-command add step-three import option (import blobs only) ([90e0b5b](https://gitlab.com/gitlab-org/container-registry/commit/90e0b5bcd8e33930855fe74490739965850ec263))
* **registry:** import-command use FullImport method, remove unsupported dangling-blobs option ([6747b06](https://gitlab.com/gitlab-org/container-registry/commit/6747b06bf981c81b9f494845819eb67656fa8237))
* **registry:** import-command: enable full registry pre import ([1fe18ea](https://gitlab.com/gitlab-org/container-registry/commit/1fe18eacd4f919902c15b7017b8c6437fcf6b7d0))
* remove online migration path HTTP metrics injection ([a865f85](https://gitlab.com/gitlab-org/container-registry/commit/a865f85badc1ac9f9d11bb4f15766d25f45fa938))
* **s3:** add object ownership config parameter ([cce2e48](https://gitlab.com/gitlab-org/container-registry/commit/cce2e48e737bc1dadd3b320ffd495f7315eeda59))

# [3.68.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.67.1-gitlab...v3.68.0-gitlab) (2023-03-01)


### Features

* **datastore:** add index to speed up lookups of manifests by tag ([069a705](https://gitlab.com/gitlab-org/container-registry/commit/069a70513c0210a8adc8f9677bde46a5f0a4892d))

## [3.67.1](https://gitlab.com/gitlab-org/container-registry/compare/v3.67.0-gitlab...v3.67.1-gitlab) (2023-03-01)


### Bug Fixes

* **db:** update metric name ([b39e0e1](https://gitlab.com/gitlab-org/container-registry/commit/b39e0e130fa51ae11b11d5ab0d6fbf1fc66cac31))

# [3.67.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.66.0-gitlab...v3.67.0-gitlab) (2023-02-15)


### Bug Fixes

* **driver:** write to in memory file in place ([73c1dd9](https://gitlab.com/gitlab-org/container-registry/commit/73c1dd9ec92faa5b3331c315216ad38c2fbb6b2a))


### Features

* temporarily log query duration for debugging ([6157779](https://gitlab.com/gitlab-org/container-registry/commit/6157779daee1997c17c7b68f0f53b0bc12d89186))

# [3.66.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.65.1-gitlab...v3.66.0-gitlab) (2023-01-26)


### Features

* **notifications:** add user type to actor in event ([88afcdc](https://gitlab.com/gitlab-org/container-registry/commit/88afcdca914e1ccf1079eefec75e03ade1004bf6))

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
