workspace(name = "rabbitmq-server")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")

http_archive(
    name = "rules_pkg",
    sha256 = "8f9ee2dc10c1ae514ee599a8b42ed99fa262b757058f65ad3c384289ff70c4b8",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.9.1/rules_pkg-0.9.1.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.9.1/rules_pkg-0.9.1.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

git_repository(
    name = "rules_erlang",
    remote = "https://github.com/rabbitmq/rules_erlang.git",
    tag = "3.12.0",
)

load("@rules_erlang//:internal_deps.bzl", "rules_erlang_internal_deps")

rules_erlang_internal_deps()

load("@rules_erlang//:internal_setup.bzl", "rules_erlang_internal_setup")

rules_erlang_internal_setup(go_repository_default_config = "//:WORKSPACE")

load("@rules_erlang//gazelle:deps.bzl", "gazelle_deps")

gazelle_deps()

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "ubuntu2004",
    registry = "index.docker.io",
    repository = "pivotalrabbitmq/ubuntu",
    tag = "20.04",
)

http_file(
    name = "openssl-3.1.1",
    downloaded_file_path = "openssl-3.1.1.tar.gz",
    sha256 = "b3aa61334233b852b63ddb048df181177c2c659eb9d4376008118f9c08d07674",
    urls = ["https://github.com/openssl/openssl/releases/download/openssl-3.1.1/openssl-3.1.1.tar.gz"],
)

http_file(
    name = "otp_src_24",
    downloaded_file_path = "OTP-24.3.4.6.tar.gz",
    sha256 = "dc3d2c54eeb093e0dc9a0fe493bc69d6dfac0affbe77c9e3c935aa86c0f63cd5",
    urls = ["https://github.com/erlang/otp/archive/OTP-24.3.4.6.tar.gz"],
)

http_file(
    name = "otp_src_25_0",
    downloaded_file_path = "OTP-25.0.4.tar.gz",
    sha256 = "05878cb51a64b33c86836b12a21903075c300409b609ad5e941ddb0feb8c2120",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.0.4.tar.gz"],
)

http_file(
    name = "otp_src_25_1",
    downloaded_file_path = "OTP-25.1.2.1.tar.gz",
    sha256 = "79f8e31bb9ff7d43a920f207ef104d1106b2332fdbadf11241d714eacb6d8d1a",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.1.2.1.tar.gz"],
)

http_file(
    name = "otp_src_25_2",
    downloaded_file_path = "OTP-25.2.3.tar.gz",
    sha256 = "637bc5cf68dd229fd3c3fe889a6f84dd32c4a827488550a0a98123b00c2d78b5",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.2.3.tar.gz"],
)

http_file(
    name = "otp_src_25_3",
    downloaded_file_path = "OTP-25.3.2.6.tar.gz",
    sha256 = "67e0f5c209a335cfc216a57b1f016072a69eb9683d36d6d101bf2f60a2e45926",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.3.2.6.tar.gz"],
)

http_file(
    name = "otp_src_26_1",
    downloaded_file_path = "OTP-26.1.1.tar.gz",
    sha256 = "a47203930e4b34a0e23bdf0a968127e5ec9d0e6c69ccf2e53be81cd2360eee2d",
    urls = ["https://github.com/erlang/otp/archive/OTP-26.1.1.tar.gz"],
)

new_git_repository(
    name = "bats",
    build_file = "@//:BUILD.bats",
    remote = "https://github.com/sstephenson/bats",
    tag = "v0.4.0",
)

load("//deps/amqp10_client:activemq.bzl", "activemq_archive")

activemq_archive()

load("//bazel/bzlmod:secondary_umbrella.bzl", "secondary_umbrella")

secondary_umbrella()

git_repository(
    name = "rbe",
    branch = "linux-rbe",
    remote = "https://github.com/rabbitmq/rbe-erlang-platform.git",
)
