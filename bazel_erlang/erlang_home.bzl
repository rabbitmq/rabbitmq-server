ErlangHomeProvider = provider(
    fields = ["path"],
)

def _impl(ctx):
    return ErlangHomeProvider(path = ctx.build_setting_value)

erlang_home = rule(
    implementation = _impl,
    # The next line marks this as a special rule that we can
    # configure when invoking the cli or via .bazelrc file
    build_setting = config.string(flag = True)
)
