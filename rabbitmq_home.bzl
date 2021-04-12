load("@bazel-erlang//:bazel_erlang_lib.bzl", "ErlangLibInfo", "flat_deps", "path_join")

RabbitmqHomeInfo = provider(
    doc = "An assembled RABBITMQ_HOME dir",
    fields = {
        "sbin": "Files making up the sbin dir",
        "escript": "Files making up the escript dir",
        "plugins": "Files making up the plugins dir",
        "erlang_version": "Version of the Erlang compiler used",
    },
)

def _copy_script(ctx, script):
    dest = ctx.actions.declare_file(path_join(ctx.label.name, "sbin", script.basename))
    ctx.actions.run_shell(
        inputs = [script],
        outputs = [dest],
        command = "mkdir -p {} && cp {} {}".format(dest.dirname, script.path, dest.path),
    )
    return dest

def _link_escript(ctx, escript):
    e = ctx.attr._rabbitmqctl_escript.files_to_run.executable
    s = ctx.actions.declare_file(path_join(ctx.label.name, "escript", escript))
    ctx.actions.symlink(
        output = s,
        target_file = e,
    )
    return s

def _priv_file_dest_relative_path(plugin_label, f):
    rel_base = plugin_label.package
    if plugin_label.workspace_root != "":
        rel_base = path_join(plugin_label.workspace_root, rel_base)
    if rel_base == "":
        return f.path
    else:
        return f.path.replace(rel_base + "/", "")

def _plugins_dir_links(ctx, plugin):
    lib_info = plugin[ErlangLibInfo]
    plugin_path = path_join(
        ctx.label.name,
        "plugins",
        lib_info.lib_name,
    )

    links = []
    for f in lib_info.include:
        o = ctx.actions.declare_file(path_join(plugin_path, "include", f.basename))
        ctx.actions.symlink(
            output = o,
            target_file = f,
        )
        links.append(o)

    for f in lib_info.beam:
        if f.is_directory:
            if f.basename != "ebin":
                fail("{} contains a directory in 'beam' that is not an ebin dir".format(lib_info.lib_name))
            o = ctx.actions.declare_file(path_join(plugin_path, "ebin"))
        else:
            o = ctx.actions.declare_file(path_join(plugin_path, "ebin", f.basename))
        ctx.actions.symlink(
            output = o,
            target_file = f,
        )
        links.append(o)

    for f in lib_info.priv:
        p = _priv_file_dest_relative_path(plugin.label, f)
        o = ctx.actions.declare_file(path_join(plugin_path, p))
        ctx.actions.symlink(
            output = o,
            target_file = f,
        )
        links.append(o)

    return links

def _unique_versions(plugins):
    erlang_versions = []
    for plugin in plugins:
        erlang_version = plugin[ErlangLibInfo].erlang_version
        if not erlang_version in erlang_versions:
            erlang_versions.append(erlang_version)
    return erlang_versions

def _flatten(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]

def _impl(ctx):
    plugins = flat_deps(ctx.attr.plugins)

    erlang_versions = _unique_versions(plugins)
    if len(erlang_versions) > 1:
        fail("plugins do not have a unified erlang version", erlang_versions)

    scripts = [_copy_script(ctx, script) for script in ctx.files._scripts]

    escripts = [_link_escript(ctx, escript) for escript in ["rabbitmq-plugins", "rabbitmqctl"]]

    plugins = _flatten([_plugins_dir_links(ctx, plugin) for plugin in plugins])

    return [
        RabbitmqHomeInfo(
            sbin = scripts,
            escript = escripts,
            plugins = plugins,
            erlang_version = erlang_versions[0],
        ),
        DefaultInfo(
            files = depset(scripts + escripts + plugins),
        ),
    ]

rabbitmq_home = rule(
    implementation = _impl,
    attrs = {
        "_scripts": attr.label_list(
            default = [
                "//deps/rabbit:scripts/rabbitmq-defaults",
                "//deps/rabbit:scripts/rabbitmq-env",
                "//deps/rabbit:scripts/rabbitmq-plugins",
                "//deps/rabbit:scripts/rabbitmq-server",
                "//deps/rabbit:scripts/rabbitmqctl",
            ],
            allow_files = True,
        ),
        "_rabbitmqctl_escript": attr.label(default = "//deps/rabbitmq_cli:rabbitmqctl"),
        "_erlang_version": attr.label(default = "@bazel-erlang//:erlang_version"),
        "plugins": attr.label_list(),
    },
)
