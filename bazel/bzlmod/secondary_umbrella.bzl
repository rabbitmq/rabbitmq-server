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
        name = "rabbitmq-server-generic-unix-3.10",
        build_file = "@//:BUILD.package_generic_unix",
        patch_cmds = [ADD_PLUGINS_DIR_BUILD_FILE],
        sha256 = "11651575d9c1b6b1803a41b5a37ad437abfb883fb7415500f98695f99943a83d",
        strip_prefix = "rabbitmq_server-3.10.6",
        urls = ["https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.10.6/rabbitmq-server-generic-unix-3.10.6.tar.xz"],
    )
