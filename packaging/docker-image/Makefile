all: dist

dist: package-generic-unix.tar
	docker buildx build --pull \
	  --platform linux/arm/v7,linux/amd64 \
	  .

clean:
	rm package-generic-unix.tar

package-generic-unix.tar:
	xzcat ../../bazel-bin/package-generic-unix.tar.xz > $@

OTP_VERSION_MATCH ?= 25[0-9.]+
define LATEST_STABLE_OTP_VERSION
curl --silent --fail https://api.github.com/repos/erlang/otp/git/refs/tags | \
  jq -r '.[].ref | sub("refs/tags/OTP.{1}";"") | match("^$(OTP_VERSION_MATCH)$$") | .string' | \
  tail -n 1
endef
.PHONY: find-otp-sha256
find-otp-sha256:
	@printf "Version: " && \
	export VERSION="$$($(LATEST_STABLE_OTP_VERSION))" && \
	echo "$$VERSION" && \
	printf "Checksum: " && \
	wget --continue --quiet --output-document="/tmp/OTP-$$VERSION.tar.gz" "https://github.com/erlang/otp/archive/OTP-$$VERSION.tar.gz" && \
	shasum -a 256 "/tmp/OTP-$$VERSION.tar.gz"
