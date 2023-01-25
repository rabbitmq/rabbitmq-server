load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")

git_repository(
    name = "rules_erlang",
    remote = "https://github.com/rabbitmq/rules_erlang.git",
    tag = "3.9.1",
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
    name = "openssl-1.1.1g",
    downloaded_file_path = "openssl-1.1.1g.tar.gz",
    sha256 = "ddb04774f1e32f0c49751e21b67216ac87852ceb056b75209af2443400636d46",
    urls = ["https://www.openssl.org/source/openssl-1.1.1g.tar.gz"],
)

http_file(
    name = "otp_src_24",
    downloaded_file_path = "OTP-24.3.4.8.tar.gz",
    sha256 = "72efa2bb1556a3eaa6afa874ec2dffb5e63d5c9c0c75090deb1d5acef928dc9e",
    urls = ["https://github.com/erlang/otp/archive/OTP-24.3.4.8.tar.gz"],
)

http_file(
    name = "otp_src_25_0",
    downloaded_file_path = "OTP-25.0.4.tar.gz",
    sha256 = "05878cb51a64b33c86836b12a21903075c300409b609ad5e941ddb0feb8c2120",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.0.4.tar.gz"],
)

http_file(
    name = "otp_src_25_1",
    downloaded_file_path = "OTP-25.1.1.tar.gz",
    sha256 = "3348616450868fa8b39bddf0b528030e4525afef5b30e3a4b54c375add7d3f4f",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.1.1.tar.gz"],
)

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "a2a5cccec251211e2221b1587af2ce43c36d32a42f5d881737db3b546a536510",
    strip_prefix = "buildbuddy-toolchain-829c8a574f706de5c96c54ca310f139f4acda7dd",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/829c8a574f706de5c96c54ca310f139f4acda7dd.tar.gz"],
)

load(
    "@rules_erlang//:rules_erlang.bzl",
    "erlang_config",
    "internal_erlang_from_github_release",
    "internal_erlang_from_http_archive",
)

erlang_config(
    internal_erlang_configs = [
        internal_erlang_from_github_release(
            name = "24",
            sha256 = "3d8ca72146cc08f1084e4e0fcaec31621108fd88ad1368f01dae3b8fb628e859",
            version = "24.3.4.8",
        ),
        internal_erlang_from_github_release(
            name = "25_0",
            sha256 = "8fc707f92a124b2aeb0f65dcf9ac8e27b2a305e7bcc4cc1b2fdf770eec0165bf",
            version = "25.0.4",
        ),
        internal_erlang_from_github_release(
            name = "25_1",
            sha256 = "42840c32e13a27bdb2c376d69aa22466513d441bfe5eb882de23baf8218308d3",
            version = "25.1.1",
        ),
        internal_erlang_from_http_archive(
            name = "git_master",
            strip_prefix = "otp-master",
            url = "https://github.com/erlang/otp/archive/refs/heads/master.tar.gz",
            version = "master",
        ),
    ],
)

load("@erlang_config//:defaults.bzl", "register_defaults")

register_defaults()

load(
    "//bazel/elixir:elixir.bzl",
    "elixir_config",
    "internal_elixir_from_github_release",
)

elixir_config(
    internal_elixir_configs = [
        internal_elixir_from_github_release(
            name = "1_13",
            sha256 = "95daf2dd3052e6ca7d4d849457eaaba09de52d65ca38d6933c65bc1cdf6b8579",
            version = "1.13.4",
        ),
        internal_elixir_from_github_release(
            name = "1_14",
            sha256 = "8ad537eb84471c24c3e6984c37884f06a7834ff2efd72c436c222baee8df9a11",
            version = "1.14.1",
        ),
    ],
    rabbitmq_server_workspace = "@",
)

load(
    "@elixir_config//:defaults.bzl",
    register_elixir_defaults = "register_defaults",
)

register_elixir_defaults()

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

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(
    name = "buildbuddy_toolchain",
    llvm = True,
)

git_repository(
    name = "rbe",
    branch = "linux-rbe",
    remote = "https://github.com/rabbitmq/rbe-erlang-platform.git",
)
