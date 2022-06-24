load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

ADD_PLUGINS_DIR_BUILD_FILE = """set -euo pipefail

cat << EOF > plugins/BUILD.bazel
load("@rules_pkg//:pkg.bzl", "pkg_zip")

pkg_zip(
    name = "inet_tcp_proxy_ez",
    package_dir = "inet_tcp_proxy/ebin",
    srcs = [
        "@inet_tcp_proxy_dist//:erlang_app",
    ],
    package_file_name = "inet_tcp_proxy-0.1.0.ez",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "standard_plugins",
    srcs = glob(["**/*"]),
    visibility = ["//visibility:public"],
)
EOF
"""

def secondary_umbrella():
    http_archive(
        name = "rabbitmq-server-generic-unix-3.7",
        build_file = "@//:BUILD.package_generic_unix",
        patch_cmds = [ADD_PLUGINS_DIR_BUILD_FILE],
        sha256 = "8cc45ef421323b407eda3fa82975fffd81d9a46235f6314e16855caedacd02cc",
        strip_prefix = "rabbitmq_server-3.7.28",
        urls = ["https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.7.28/rabbitmq-server-generic-unix-3.7.28.tar.xz"],
    )
