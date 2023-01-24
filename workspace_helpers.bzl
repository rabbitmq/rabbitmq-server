load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    new_git_repository(
        name = "bats",
        remote = "https://github.com/sstephenson/bats",
        tag = "v0.4.0",
        build_file = rabbitmq_workspace + "//:BUILD.bats",
    )
