def _impl(ctx):
    script = """set -euo pipefail

external/bats/libexec/bats {test_files}
""".format(
        package_dir = ctx.label.package,
        test_files = " ".join([t.short_path for t in ctx.files.srcs]),
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(ctx.files.bats + ctx.files.srcs + ctx.files.data)
    return [DefaultInfo(runfiles = runfiles)]

bats_test = rule(
    implementation = _impl,
    attrs = {
        "bats": attr.label(),
        "srcs": attr.label_list(
            allow_files = [".bats"],
            mandatory = True,
        ),
        "data": attr.label_list(allow_files = True),
    },
    test = True,
)

def bats(**kwargs):
    bats_test(
        name = "bats",
        bats = "@bats//:bin_dir",
        **kwargs
    )
