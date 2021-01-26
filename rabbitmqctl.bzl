load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

def _impl(ctx):
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
        "home": attr.label(providers=[RabbitmqHomeInfo]),
    },
    executable = True,
)
