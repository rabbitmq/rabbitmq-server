load("//bazel_erlang:erlang_home.bzl", "ErlangVersionProvider", "ErlangHomeProvider")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

def _impl(ctx):
    rabbitmq_home = ctx.attr.home[RabbitmqHomeInfo]

    if rabbitmq_home.erlang_version != ctx.attr._erlang_version[ErlangVersionProvider].version:
        fail()

    script = """
    cd {}/{}
    exec ./sbin/rabbitmqctl $@ 
    """.format(ctx.attr.home.label.name, rabbitmq_home.erlang_version)

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
        "_erlang_version": attr.label(default = "//bazel_erlang:erlang_version"),
        "home": attr.label(providers=[RabbitmqHomeInfo]),
    },
    executable = True,
)
