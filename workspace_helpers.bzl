load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel-erlang//:github.bzl", "github_bazel_erlang_lib")
load("@bazel-erlang//:hex_pm.bzl", "hex_pm_bazel_erlang_lib")
load("//:rabbitmq.bzl", "APP_VERSION")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    github_bazel_erlang_lib(
        name = "aten",
        org = "rabbitmq",
        sha256 = "27f6b2ec2e78027ea852a8ac6bcf49df4a599d5506a86dc9f0cb6b5d6e45989e",
        ref = "v0.5.6",
        version = "0.5.6",
    )

    hex_pm_bazel_erlang_lib(
        name = "cowboy",
        first_srcs = [
            "src/cowboy_stream.erl",
            "src/cowboy_middleware.erl",
            "src/cowboy_sub_protocol.erl",
        ],
        version = "2.8.0",
        deps = [
            "@cowlib//:bazel_erlang_lib",
            "@ranch//:bazel_erlang_lib",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "cowlib",
        version = "2.9.1",
    )

    github_bazel_erlang_lib(
        repo = "credentials-obfuscation",
        name = "credentials_obfuscation",
        org = "rabbitmq",
        sha256 = "a5cecd861334a8a5fb8c9b108a74c83ba0041653c53c523bb97f70dbefa30fe3",
        ref = "v2.4.0",
        version = "2.4.0",
    )

    github_bazel_erlang_lib(
        name = "cuttlefish",
        org = "Kyorai",
    )

    github_bazel_erlang_lib(
        repo = "gen-batch-server",
        name = "gen_batch_server",
        org = "rabbitmq",
        sha256 = "9e9f2aa6ee8e3354f03a3f78283fde93bbe5b1d6f6732caa05d3e43efe02e42c",
        ref = "v0.8.4",
        version = "0.8.4",
    )

    http_archive(
        name = "inet_tcp_proxy",
        build_file = rabbitmq_workspace + "//:BUILD.inet_tcp_proxy",
        strip_prefix = "inet_tcp_proxy-master",
        urls = ["https://github.com/rabbitmq/inet_tcp_proxy/archive/master.zip"],
    )

    hex_pm_bazel_erlang_lib(
        name = "jsx",
        version = "2.11.0",
        erlc_opts = [
            "+debug_info",
            "-Dmaps_support=1",
        ],
    )

    github_bazel_erlang_lib(
        name = "meck",
        org = "eproxus",
    )

    hex_pm_bazel_erlang_lib(
        name = "observer_cli",
        version = "1.6.1",
    )

    http_archive(
        name = "osiris",
        build_file = rabbitmq_workspace + "//:BUILD.osiris",
        strip_prefix = "osiris-master",
        urls = ["https://github.com/rabbitmq/osiris/archive/master.zip"],
    )

    github_bazel_erlang_lib(
        name = "proper",
        first_srcs = ["src/vararg.erl"],
        org = "manopapad",
    )

    http_archive(
        name = "ra",
        build_file = rabbitmq_workspace + "//:BUILD.ra",
        strip_prefix = "ra-master",
        urls = ["https://github.com/rabbitmq/ra/archive/master.zip"],
    )

    hex_pm_bazel_erlang_lib(
        name = "ranch",
        first_srcs = [
            "src/ranch_transport.erl",
        ],
        version = "2.0.0",
    )

    hex_pm_bazel_erlang_lib(
        name = "recon",
        version = "2.5.1",
    )

    hex_pm_bazel_erlang_lib(
        name = "stdout_formatter",
        version = "0.2.4",
    )

    github_bazel_erlang_lib(
        name = "syslog",
        org = "schlagert",
        sha256 = "25abcfe2cc0745fc4ffb0d66d4a5868d343a0130c7a7ddcae03771326feae619",
        ref = "3.4.5",
        version = "3.4.5",
        first_srcs = [
            "src/syslog_logger.erl",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "sysmon_handler",
        version = "1.3.0",
    )
