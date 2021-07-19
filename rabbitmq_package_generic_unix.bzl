load("@//:rabbitmq_home.bzl", "RabbitmqHomeInfo")

def _impl(ctx):
    scripts = ctx.files.sbin
    escripts = ctx.files.escript
    plugins = ctx.files.plugins

    return [
        RabbitmqHomeInfo(
            sbin = scripts,
            escript = escripts,
            plugins = plugins,
        ),
        DefaultInfo(
            files = depset(scripts + escripts + plugins),
        ),
    ]

rabbitmq_package_generic_unix = rule(
    implementation = _impl,
    attrs = {
        "sbin": attr.label_list(allow_files = True),
        "escript": attr.label_list(allow_files = True),
        "plugins": attr.label_list(allow_files = True),
    },
)
