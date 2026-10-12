FROM elixir:1.20-otp-29

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      7zip ca-certificates git make rsync xz-utils zip unzip openssl netcat-openbsd python3 \
    && rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

COPY . /src
WORKDIR /src

RUN make

RUN set -e; for d in $(git ls-tree -d --name-only HEAD deps/); do \
      if [ -f "$d/Makefile" ] && [ -d "$d/test" ]; then make -C "$d" test-build; fi; \
    done
