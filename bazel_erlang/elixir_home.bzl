ElixirHomeProvider = provider(
    fields = ["path"],
)

MixArchivesProvider = provider(
    fields = ["path"],
)

def _elixir_home_impl(ctx):
    return ElixirHomeProvider(path = ctx.build_setting_value)

elixir_home = rule(
    implementation = _elixir_home_impl,
    # The next line marks this as a special rule that we can
    # configure when invoking the cli or via .bazelrc file
    build_setting = config.string(flag = True)
)

def _mix_archives_impl(ctx):
    return MixArchivesProvider(path = ctx.build_setting_value)

mix_archives = rule(
    implementation = _mix_archives_impl,
    build_setting = config.string(flag = True)
)