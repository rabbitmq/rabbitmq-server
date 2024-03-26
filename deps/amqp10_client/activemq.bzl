load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

ACTIVEMQ_VERSION = "5.18.3"
ACTIVEMQ_URL = "https://archive.apache.org/dist/activemq/{version}/apache-activemq-{version}-bin.tar.gz".format(version = ACTIVEMQ_VERSION)
SHA_256 = "943381aa6d340707de6c42eadbf7b41b7fdf93df604156d972d50c4da783544f"

def activemq_archive():
    http_archive(
        name = "activemq",
        urls = [ACTIVEMQ_URL],
        sha256 = SHA_256,
        strip_prefix = "apache-activemq-{}".format(ACTIVEMQ_VERSION),
        build_file_content = """filegroup(
    name = "exec_dir",
    srcs = glob(["bin/**/*", "lib/**/*", "conf/**/*", "activemq-all-*.jar"]),
    visibility = ["//visibility:public"],
)
""",
    )
