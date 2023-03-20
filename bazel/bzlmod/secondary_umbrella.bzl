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
        name = "rabbitmq-server-generic-unix-3.11",
        build_file = "@//:BUILD.package_generic_unix",
        patch_cmds = [ADD_PLUGINS_DIR_BUILD_FILE],
        strip_prefix = "rabbitmq_server-3.11.11",
        # This file is produced just in time by the test-mixed-versions.yaml GitHub Actions workflow.
        urls = [
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/package-generic-unix-for-mixed-version-testing-v3.11.11.tar.xz",
        ],
    )
