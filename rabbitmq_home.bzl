load("@rules_erlang//:erlang_app_info.bzl", "ErlangAppInfo", "flat_deps")
load("@rules_erlang//:util.bzl", "path_join")
load("@rules_erlang//:ct.bzl", "additional_file_dest_relative_path")

RabbitmqHomeInfo = provider(
    doc = "An assembled RABBITMQ_HOME dir",
    fields = {
        "rabbitmqctl": "rabbitmqctl script from the sbin directory",
    },
)

def _copy_script(ctx, script):
    dest = ctx.actions.declare_file(path_join(ctx.label.name, "sbin", script.basename))
    ctx.actions.expand_template(
        template = script,
        output = dest,
        substitutions = {},
    )
    return dest

def link_escript(ctx, escript):
    e = ctx.attr._rabbitmqctl_escript.files_to_run.executable
    s = ctx.actions.declare_file(path_join(ctx.label.name, "escript", escript))
    ctx.actions.symlink(
        output = s,
        target_file = e,
    )
    return s

def _plugins_dir_links(ctx, plugin):
    lib_info = plugin[ErlangAppInfo]
    plugin_path = path_join(
        ctx.label.name,
        "plugins",
        lib_info.app_name,
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
        p = additional_file_dest_relative_path(plugin.label, f)
        o = ctx.actions.declare_file(path_join(plugin_path, p))
        ctx.actions.symlink(
            output = o,
            target_file = f,
        )
        links.append(o)

    return links

def unique_versions(plugins):
    erlang_versions = []
    for plugin in plugins:
        erlang_version = plugin[ErlangAppInfo].erlang_version
        if not erlang_version in erlang_versions:
            erlang_versions.append(erlang_version)
    return erlang_versions

def flatten(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]

def _impl(ctx):
    plugins = flat_deps(ctx.attr.plugins)

    erlang_versions = unique_versions(plugins)
    if len(erlang_versions) > 1:
        fail("plugins do not have a unified erlang version", erlang_versions)

    if not ctx.attr.is_windows:
        source_scripts = ctx.files._scripts
    else:
        source_scripts = ctx.files._scripts_windows
    scripts = [_copy_script(ctx, script) for script in source_scripts]

    rabbitmq_ctl_copies = [
        "rabbitmq-diagnostics",
        "rabbitmq-plugins",
        "rabbitmq-queues",
        "rabbitmq-streams",
        "rabbitmq-tanzu",
        "rabbitmq-upgrade",
        "rabbitmqctl",
    ]
    escripts = [link_escript(ctx, escript) for escript in rabbitmq_ctl_copies]

    plugins = flatten([_plugins_dir_links(ctx, plugin) for plugin in plugins])

    rabbitmqctl = None
    for script in scripts:
        if script.basename == ("rabbitmqctl" if not ctx.attr.is_windows else "rabbitmqctl.bat"):
            rabbitmqctl = script
    if rabbitmqctl == None:
        fail("could not find rabbitmqctl among", scripts)

    return [
        RabbitmqHomeInfo(
            rabbitmqctl = rabbitmqctl,
        ),
        DefaultInfo(
            files = depset(scripts + escripts + plugins),
        ),
    ]

RABBITMQ_HOME_ATTRS = {
    "_scripts": attr.label_list(
        default = [
            "//deps/rabbit:scripts/rabbitmq-defaults",
            "//deps/rabbit:scripts/rabbitmq-diagnostics",
            "//deps/rabbit:scripts/rabbitmq-env",
            "//deps/rabbit:scripts/rabbitmq-plugins",
            "//deps/rabbit:scripts/rabbitmq-queues",
            "//deps/rabbit:scripts/rabbitmq-server",
            "//deps/rabbit:scripts/rabbitmqctl",
        ],
        allow_files = True,
    ),
    "_scripts_windows": attr.label_list(
        default = [
            "//deps/rabbit:scripts/rabbitmq-defaults.bat",
            "//deps/rabbit:scripts/rabbitmq-diagnostics.bat",
            "//deps/rabbit:scripts/rabbitmq-env.bat",
            "//deps/rabbit:scripts/rabbitmq-plugins.bat",
            "//deps/rabbit:scripts/rabbitmq-queues.bat",
            "//deps/rabbit:scripts/rabbitmq-server.bat",
            "//deps/rabbit:scripts/rabbitmqctl.bat",
        ],
        allow_files = True,
    ),
    "_rabbitmqctl_escript": attr.label(default = "//deps/rabbitmq_cli:rabbitmqctl"),
    "is_windows": attr.bool(mandatory = True),
    "plugins": attr.label_list(providers = [ErlangAppInfo]),
}

rabbitmq_home_private = rule(
    implementation = _impl,
    attrs = RABBITMQ_HOME_ATTRS,
)

def rabbitmq_home(**kwargs):
    rabbitmq_home_private(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )

def _dirname(p):
    return p.rpartition("/")[0]

def rabbitmq_home_short_path(rabbitmq_home):
    short_path = rabbitmq_home[RabbitmqHomeInfo].rabbitmqctl.short_path
    if rabbitmq_home.label.workspace_root != "":
        short_path = path_join(rabbitmq_home.label.workspace_root, short_path)
    return _dirname(_dirname(short_path))
