package storage

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/testutil"
	digest "github.com/opencontainers/go-digest"
)

func TestUploadIsConfigPayload(t *testing.T) {
	fooRepoName, _ := reference.WithName("bw/configpayload")
	env := newManifestStoreTestEnv(t, fooRepoName, "TestUploadIsConfigPayload")

	var tests = []struct {
		layerFunc func(*testing.T) (io.ReadSeeker, digest.Digest)
		expected  bool
	}{
		{func(t *testing.T) (io.ReadSeeker, digest.Digest) {
			rs, ds, err := testutil.CreateRandomTarFile()
			if err != nil {
				t.Fatalf("generating test layer file: %v", err)
			}
			return rs, ds
		}, false},
		{func(t *testing.T) (io.ReadSeeker, digest.Digest) {
			rs := bytes.NewReader([]byte{})
			return rs, digest.FromBytes([]byte{})
		}, false},
		{func(t *testing.T) (io.ReadSeeker, digest.Digest) {
			dockerPayload := `{"architecture":"amd64","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"ExposedPorts":{"6379/tcp":{}},"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin","REDIS_VERSION=4.0.14","REDIS_DOWNLOAD_URL=http://download.redis.io/releases/redis-4.0.14.tar.gz","REDIS_DOWNLOAD_SHA=1e1e18420a86cfb285933123b04a82e1ebda20bfb0a289472745a087587e93a7"],"Cmd":["redis-server"],"ArgsEscaped":true,"Image":"sha256:65e0eab145ea3df4a4cc9bc2f08c0ede791bc1d65b210a5359618007b0b6d8e9","Volumes":{"/data":{}},"WorkingDir":"/data","Entrypoint":["docker-entrypoint.sh"],"OnBuild":null,"Labels":null},"container":"81f11db016a0dd3ec947c5fa8f8f4c52a72f9e582e8364f6c4ead301ba94dd60","container_config":{"Hostname":"81f11db016a0","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"ExposedPorts":{"6379/tcp":{}},"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin","REDIS_VERSION=4.0.14","REDIS_DOWNLOAD_URL=http://download.redis.io/releases/redis-4.0.14.tar.gz","REDIS_DOWNLOAD_SHA=1e1e18420a86cfb285933123b04a82e1ebda20bfb0a289472745a087587e93a7"],"Cmd":["/bin/sh","-c","#(nop) ","CMD [\"redis-server\"]"],"ArgsEscaped":true,"Image":"sha256:65e0eab145ea3df4a4cc9bc2f08c0ede791bc1d65b210a5359618007b0b6d8e9","Volumes":{"/data":{}},"WorkingDir":"/data","Entrypoint":["docker-entrypoint.sh"],"OnBuild":null,"Labels":{}},"created":"2020-03-24T00:53:36.48646124Z","docker_version":"18.09.7","history":[{"created":"2020-03-23T21:19:34.027725872Z","created_by":"/bin/sh -c #(nop) ADD file:0c4555f363c2672e350001f1293e689875a3760afe7b3f9146886afe67121cba in / "},{"created":"2020-03-23T21:19:34.196162891Z","created_by":"/bin/sh -c #(nop)  CMD [\"/bin/sh\"]","empty_layer":true},{"created":"2020-03-24T00:49:58.642637457Z","created_by":"/bin/sh -c addgroup -S -g 1000 redis \u0026\u0026 adduser -S -G redis -u 999 redis"},{"created":"2020-03-24T00:49:59.878878083Z","created_by":"/bin/sh -c apk add --no-cache \t\t'su-exec\u003e=0.2' \t\ttzdata"},{"created":"2020-03-24T00:53:01.203081664Z","created_by":"/bin/sh -c #(nop)  ENV REDIS_VERSION=4.0.14","empty_layer":true},{"created":"2020-03-24T00:53:01.409019557Z","created_by":"/bin/sh -c #(nop)  ENV REDIS_DOWNLOAD_URL=http://download.redis.io/releases/redis-4.0.14.tar.gz","empty_layer":true},{"created":"2020-03-24T00:53:01.598823196Z","created_by":"/bin/sh -c #(nop)  ENV REDIS_DOWNLOAD_SHA=1e1e18420a86cfb285933123b04a82e1ebda20bfb0a289472745a087587e93a7","empty_layer":true},{"created":"2020-03-24T00:53:34.412554596Z","created_by":"/bin/sh -c set -eux; \t\tapk add --no-cache --virtual .build-deps \t\tcoreutils \t\tgcc \t\tlinux-headers \t\tmake \t\tmusl-dev \t\topenssl-dev \t; \t\twget -O redis.tar.gz \"$REDIS_DOWNLOAD_URL\"; \techo \"$REDIS_DOWNLOAD_SHA *redis.tar.gz\" | sha256sum -c -; \tmkdir -p /usr/src/redis; \ttar -xzf redis.tar.gz -C /usr/src/redis --strip-components=1; \trm redis.tar.gz; \t\tgrep -q '^#define CONFIG_DEFAULT_PROTECTED_MODE 1$' /usr/src/redis/src/server.h; \tsed -ri 's!^(#define CONFIG_DEFAULT_PROTECTED_MODE) 1$!\\1 0!' /usr/src/redis/src/server.h; \tgrep -q '^#define CONFIG_DEFAULT_PROTECTED_MODE 0$' /usr/src/redis/src/server.h; \t\tmake -C /usr/src/redis -j \"$(nproc)\" all; \tmake -C /usr/src/redis install; \t\tserverMd5=\"$(md5sum /usr/local/bin/redis-server | cut -d' ' -f1)\"; export serverMd5; \tfind /usr/local/bin/redis* -maxdepth 0 \t\t-type f -not -name redis-server \t\t-exec sh -eux -c ' \t\t\tmd5=\"$(md5sum \"$1\" | cut -d\" \" -f1)\"; \t\t\ttest \"$md5\" = \"$serverMd5\"; \t\t' -- '{}' ';' \t\t-exec ln -svfT 'redis-server' '{}' ';' \t; \t\trm -r /usr/src/redis; \t\trunDeps=\"$( \t\tscanelf --needed --nobanner --format '%n#p' --recursive /usr/local \t\t\t| tr ',' '\\n' \t\t\t| sort -u \t\t\t| awk 'system(\"[ -e /usr/local/lib/\" $1 \" ]\") == 0 { next } { print \"so:\" $1 }' \t)\"; \tapk add --no-network --virtual .redis-rundeps $runDeps; \tapk del --no-network .build-deps; \t\tredis-cli --version; \tredis-server --version"},{"created":"2020-03-24T00:53:35.219227357Z","created_by":"/bin/sh -c mkdir /data \u0026\u0026 chown redis:redis /data"},{"created":"2020-03-24T00:53:35.427328104Z","created_by":"/bin/sh -c #(nop)  VOLUME [/data]","empty_layer":true},{"created":"2020-03-24T00:53:35.651172162Z","created_by":"/bin/sh -c #(nop) WORKDIR /data","empty_layer":true},{"created":"2020-03-24T00:53:35.927395676Z","created_by":"/bin/sh -c #(nop) COPY file:c48b97ea65422782310396358f838c38c0747767dd606a88d4c3d0b034a60762 in /usr/local/bin/ "},{"created":"2020-03-24T00:53:36.114454222Z","created_by":"/bin/sh -c #(nop)  ENTRYPOINT [\"docker-entrypoint.sh\"]","empty_layer":true},{"created":"2020-03-24T00:53:36.298359539Z","created_by":"/bin/sh -c #(nop)  EXPOSE 6379","empty_layer":true},{"created":"2020-03-24T00:53:36.48646124Z","created_by":"/bin/sh -c #(nop)  CMD [\"redis-server\"]","empty_layer":true}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:beee9f30bc1f711043e78d4a2be0668955d4b761d587d6f60c2c8dc081efb203","sha256:bc5a81440134d738e909891494d42f7262a4e4cf8cc73293c28a654d3ba146cc","sha256:5212569ac346b490d48eb23f5511cf0f0303728d21f2d60677f8bd5a84a11460","sha256:692bf7906cbbb647e7381216bb5e1d708ca2a05998cbfccca6def4c3f6a9fccd","sha256:068ccc121d6dd4c0bb03dcd3b3793b39b2fa63ffcf606540f14f01856d345417","sha256:baf6c482be2e888923e7201e56ed111c019f47e84a05ed08824b7d8d03338038"]}}`
			rs := strings.NewReader(dockerPayload)
			return rs, digest.FromString(dockerPayload)
		}, true},
		{func(t *testing.T) (io.ReadSeeker, digest.Digest) {
			helmPayload := `{"name":"e-helm","version":"latest","description":"Sample Helm Chart","apiVersion":"v2","appVersion":"1.16.0","type":"application"}`
			rs := strings.NewReader(helmPayload)
			return rs, digest.FromString(helmPayload)
		}, true},
		{func(t *testing.T) (io.ReadSeeker, digest.Digest) {
			malformedPayload := `{"invalid":"json",`
			rs := strings.NewReader(malformedPayload)
			return rs, digest.FromString(malformedPayload)
		}, false},
		{func(t *testing.T) (io.ReadSeeker, digest.Digest) {
			malformedPayload := "unformatted string"
			rs := strings.NewReader(malformedPayload)
			return rs, digest.FromString(malformedPayload)
		}, false},
	}

	for i, tt := range tests {
		wr, err := env.repository.Blobs(env.ctx).Create(env.ctx)
		if err != nil {
			t.Fatalf("test %d: creating test upload: %v", i, err)
		}

		layer, dgst := tt.layerFunc(t)

		if _, err := io.Copy(wr, layer); err != nil {
			t.Fatalf("test %d: copying to upload: %v", i, err)
		}

		// Cast to blobWritter so we can access unexported methods.
		bw, ok := wr.(*blobWriter)
		if !ok {
			t.Fatalf("test %d: unable to convert to concrete blobWriter", i)
		}

		actual, err := bw.uploadIsConfigPayload(env.ctx, distribution.Descriptor{Digest: dgst, Size: bw.fileWriter.Size()})
		if err != nil {
			t.Fatalf("test %d: checking for config payload: %v", i, err)
		}

		if actual != tt.expected {
			t.Fatalf("test %d: expected %v, got %v", i, tt.expected, actual)
		}

		// Ensure Commit continues without failure, even if we're not directly testing it.
		if _, err := wr.Commit(env.ctx, distribution.Descriptor{Digest: dgst}); err != nil {
			t.Fatalf("test %d: finishing upload: %v", i, err)
		}
	}
}
