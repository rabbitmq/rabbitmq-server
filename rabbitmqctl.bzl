load(":rabbitmq_home.bzl", "RabbitmqHomeInfo", "rabbitmq_home_short_path")

def _impl(ctx):
    rabbitmq_home_path = rabbitmq_home_short_path(ctx.attr.home)

    script = """
    exec ./{home}/sbin/{cmd} "$@"
    """.format(
        home = rabbitmq_home_path,
        cmd = ctx.label.name,
    )

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
        "home": attr.label(providers = [RabbitmqHomeInfo]),
    },
    executable = True,
)
