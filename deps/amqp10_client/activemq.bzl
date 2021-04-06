load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

ACTIVEMQ_VERSION = "5.14.4"
ACTIVEMQ_URL = "https://archive.apache.org/dist/activemq/{version}/apache-activemq-{version}-bin.tar.gz".format(version = ACTIVEMQ_VERSION)
SHA_256 = "16ec52bece0a4759f9d70f4132d7d8da67d662e4af029081c492e65510a695c1"

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
