load("//bazel_erlang:erlang_home.bzl", "ErlangHomeProvider")
load(":rabbitmq_home.bzl", "RabbitmqHomeInfo")

# Note: Theses rules take advantage of the fact that when the files from
#       the rabbitmq_home rule are used as runfiles, they are linked in
#       at their declared relative paths. In other words, since
#       rabbitmq_home declares "sbin/rabbitmq-server", is still at
#       "sbin/rabbitmq-server" when our script runs.

def _impl(ctx):
    rabbitmq_home = ctx.attr.home[RabbitmqHomeInfo]

    script = """
    exec ./sbin/rabbitmqctl $@ 
    """

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
