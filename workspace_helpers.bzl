load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@rules_erlang//:github.bzl", "github_erlang_app")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    new_git_repository(
        name = "bats",
        remote = "https://github.com/sstephenson/bats",
        tag = "v0.4.0",
        build_file = rabbitmq_workspace + "//:BUILD.bats",
    )

    http_archive(
        name = "inet_tcp_proxy_dist",
        build_file = rabbitmq_workspace + "//:BUILD.inet_tcp_proxy",
        strip_prefix = "inet_tcp_proxy-master",
        urls = ["https://github.com/rabbitmq/inet_tcp_proxy/archive/master.zip"],
    )

    github_erlang_app(
        name = "meck",
        org = "eproxus",
    )

    git_repository(
        name = "osiris",
        tag = "v1.4.3",
        remote = "https://github.com/rabbitmq/osiris.git",
    )

    github_erlang_app(
        name = "proper",
        org = "manopapad",
    )

    github_erlang_app(
        name = "emqtt",
        org = "emqx",
        repo = "emqtt",
        version = "1.7.0-rc.2",
        ref = "1.7.0-rc.2",
        build_file_content = """load("@rules_erlang//:erlang_app.bzl", "erlang_app")

erlang_app(
    app_name = "emqtt",
    erlc_opts = [
        "+deterministic",
        "+debug_info",
        "-DBUILD_WITHOUT_QUIC",
    ],
)
""",
    )
