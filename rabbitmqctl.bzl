load("@bazel-erlang//:erlang_home.bzl", "ErlangVersionProvider")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

def _impl(ctx):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version

    rabbitmq_home = ctx.attr.home[RabbitmqHomeInfo]

    script = """
    exec ./{}/sbin/rabbitmqctl $@
    """.format(ctx.attr.home.label.name)

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    return [DefaultInfo(
        runfiles = ctx.runfiles(ctx.attr.home[DefaultInfo].files.to_list()),
    )]

rabbitmqctl = rule(
    implementation = _impl,
    attrs = {
        "_erlang_version": attr.label(default = "@bazel-erlang//:erlang_version"),
        "home": attr.label(providers = [RabbitmqHomeInfo]),
    },
    executable = True,
)
