load("@//:rabbitmq_home.bzl", "RabbitmqHomeInfo")

def _impl(ctx):
    return [
        RabbitmqHomeInfo(
            rabbitmqctl = ctx.file.rabbitmqctl,
        ),
        DefaultInfo(
            files = depset(ctx.files.rabbitmqctl + ctx.files.additional_files),
        ),
    ]

rabbitmq_package_generic_unix = rule(
    implementation = _impl,
    attrs = {
        "rabbitmqctl": attr.label(allow_single_file = True),
        "additional_files": attr.label_list(allow_files = True),
    },
)
