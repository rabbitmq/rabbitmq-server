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
        strip_prefix = "rabbitmq_server-3.10.24",
        urls = [
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/rbe-25/package-generic-unix-for-mixed-version-testing-v3.10.24.tar.xz",
=======
=======
>>>>>>> 91665228cc (fixup backport)
<<<<<<< HEAD
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/rbe-25_0/package-generic-unix-for-mixed-version-testing-v3.11.18.tar.xz",
=======
<<<<<<< HEAD
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/rbe-25_3/package-generic-unix-for-mixed-version-testing-v3.12.6.tar.xz",
=======
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/26.1/package-generic-unix-for-mixed-version-testing-v3.13.1.tar.xz",
>>>>>>> a2709dfd05 (Remove remaining buildbuddy usage)
>>>>>>> b38dc84db5 (Remove remaining buildbuddy usage)
<<<<<<< HEAD
>>>>>>> f28ec6c5ed (Remove remaining buildbuddy usage)
=======
=======
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/25.3/package-generic-unix-for-mixed-version-testing-v3.12.6.tar.xz",
>>>>>>> f28089177a (fixup backport)
>>>>>>> 91665228cc (fixup backport)
=======
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/rbe-25_0/package-generic-unix-for-mixed-version-testing-v3.11.18.tar.xz"
>>>>>>> 91b4d0f5b1 (Resolve a conflict #11488 #11478 #11500)
=======
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/25.0/package-generic-unix-for-mixed-version-testing-v3.11.18.tar.xz",
>>>>>>> 76e0da11d7 (Secondary umbrella: resolve a conflict)
        ],
    )
