load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

def _impl(ctx):
    rabbitmq_home = ctx.attr.home[RabbitmqHomeInfo]

    if rabbitmq_home.erlang_version != ctx.attr.erlang_version:
        fail("Mismatched erlang versions", ctx.attr.erlang_version, rabbitmq_home.erlang_version)

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
        "erlang_version": attr.string(mandatory = True),
        "home": attr.label(providers=[RabbitmqHomeInfo]),
    },
    executable = True,
)
