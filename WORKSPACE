workspace(name = "rabbitmq-server")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")

http_archive(
    name = "rules_pkg",
    sha256 = "d250924a2ecc5176808fc4c25d5cf5e9e79e6346d79d5ab1c493e289e722d1d0",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.10.1/rules_pkg-0.10.1.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.10.1/rules_pkg-0.10.1.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

git_repository(
    name = "rules_erlang",
    remote = "https://github.com/rabbitmq/rules_erlang.git",
    tag = "3.15.0",
)

load("@rules_erlang//:internal_deps.bzl", "rules_erlang_internal_deps")

rules_erlang_internal_deps()

load("@rules_erlang//:internal_setup.bzl", "rules_erlang_internal_setup")

rules_erlang_internal_setup(go_repository_default_config = "//:WORKSPACE")

load("@rules_erlang//gazelle:deps.bzl", "gazelle_deps")

gazelle_deps()

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
    downloaded_file_path = "OTP-25.3.2.10.tar.gz",
    sha256 = "be76f05bd38c60df056ed35f01085f088474a1942ce1778c2217e5658d435b35",
    urls = ["https://github.com/erlang/otp/archive/OTP-25.3.2.10.tar.gz"],
)

http_file(
    name = "otp_src_26",
    downloaded_file_path = "OTP-26.0.2.tar.gz",
    sha256 = "4def5ed5e49815fb02fceae8a66e94abc1049f5de30f97d9ad12fdf3293a2470",
    urls = ["https://github.com/erlang/otp/archive/OTP-26.0.2.tar.gz"],
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
