ErlangVersionProvider = provider(
    fields = ["version"],
)

ErlangHomeProvider = provider(
    fields = ["path"],
)

def _erlang_version_impl(ctx):
    return ErlangVersionProvider(version = ctx.build_setting_value)

erlang_version = rule(
    implementation = _erlang_version_impl,
    # The next line marks this as a special rule that we can
    # configure when invoking the cli or via .bazelrc file
    build_setting = config.string(flag = True)
)

def _erlang_home_impl(ctx):
    return ErlangHomeProvider(path = ctx.build_setting_value)

erlang_home = rule(
    implementation = _erlang_home_impl,
    # The next line marks this as a special rule that we can
    # configure when invoking the cli or via .bazelrc file
    build_setting = config.string(flag = True)
)
