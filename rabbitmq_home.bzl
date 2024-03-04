load("@rules_erlang//:erlang_app_info.bzl", "ErlangAppInfo", "flat_deps")
load("@rules_erlang//:util.bzl", "path_join")
load("@rules_erlang//:ct.bzl", "additional_file_dest_relative_path")
load("@rules_erlang//:extract_many.bzl", "extract_many_transitive")

RabbitmqHomeInfo = provider(
    doc = "An assembled RABBITMQ_HOME dir",
    fields = {
        "rabbitmqctl": "rabbitmqctl script from the sbin directory",
    },
)

def _copy_script(ctx, script):
    dest = ctx.actions.declare_file(
        path_join(ctx.label.name, "sbin", script.basename),
    )
    ctx.actions.expand_template(
        template = script,
        output = dest,
        substitutions = {},
        is_executable = True,
    )
    return dest

def copy_escript(ctx, escript):
    e = ctx.attr._rabbitmqctl_escript.files_to_run.executable
    dest = ctx.actions.declare_file(
        path_join(ctx.label.name, "escript", escript.basename),
    )
    ctx.actions.run(
        inputs = [e],
        outputs = [dest],
        executable = "cp",
        arguments = [e.path, dest.path],
    )
    return dest

def _plugins_dir_links(ctx, plugin):
    lib_info = plugin[ErlangAppInfo]
    plugin_path = path_join(
        ctx.label.name,
        "plugins",
        lib_info.app_name,
    )

    links = []
    for f in lib_info.include:
        p = additional_file_dest_relative_path(plugin.label, f)
        o = ctx.actions.declare_file(path_join(plugin_path, p))
        ctx.actions.symlink(
            output = o,
            target_file = f,
        )
        links.append(o)

    for f in lib_info.beam:
        if f.is_directory:
            if f.basename != "ebin":
                fail("{} contains a directory in 'beam' that is not an ebin dir".format(lib_info.lib_name))
            o = ctx.actions.declare_directory(path_join(plugin_path, "ebin"))
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

def flatten(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]

def _impl(ctx):
    if not ctx.attr.is_windows:
        source_scripts = ctx.files._scripts
    else:
        source_scripts = ctx.files._scripts_windows
    scripts = [_copy_script(ctx, script) for script in source_scripts]

    escripts = [copy_escript(ctx, escript) for escript in ctx.files._escripts]

    # instead of having plugins with the ErlangAppInfo Provider, since we
    # no longer know the transitive deps during the analysis phase, we
    # instead use a list of CompileManyInfo and take the plugins as a list
    # of strings.
    # We must however, still determine the transitive deps as we build the
    # plugins dir. This will mean recursively reading the .app files.

    # we need to expand the plugins_dir tar to where plugins_dir_links goes
    plugins_dir = ctx.actions.declare_directory(path_join(ctx.label.name, "plugins"))
    ctx.actions.run_shell(
        inputs = [ctx.file.plugins_dir],
        outputs = [plugins_dir],
        command = """set -euxo pipefail

tar -x \\
    -v \\
    -f {tar} \\
    -C {dest}
""".format(
            tar = ctx.file.plugins_dir.path,
            dest = plugins_dir.path,
        ),
    )

    # plugins = flatten([_plugins_dir_links(ctx, plugin) for plugin in plugins])

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
            files = depset(scripts + escripts + [plugins_dir]),
        ),
    ]

RABBITMQ_HOME_ATTRS = {
    "_escripts": attr.label_list(
        default = [
            "//deps/rabbit:scripts/rabbitmq-diagnostics",
            "//deps/rabbit:scripts/rabbitmq-plugins",
            "//deps/rabbit:scripts/rabbitmq-queues",
            "//deps/rabbit:scripts/rabbitmq-streams",
            "//deps/rabbit:scripts/rabbitmq-upgrade",
            "//deps/rabbit:scripts/rabbitmqctl",
            "//deps/rabbit:scripts/vmware-rabbitmq",
        ],
        allow_files = True,
    ),
    "_scripts": attr.label_list(
        default = [
            "//deps/rabbit:scripts/rabbitmq-defaults",
            "//deps/rabbit:scripts/rabbitmq-diagnostics",
            "//deps/rabbit:scripts/rabbitmq-env",
            "//deps/rabbit:scripts/rabbitmq-plugins",
            "//deps/rabbit:scripts/rabbitmq-queues",
            "//deps/rabbit:scripts/rabbitmq-server",
            "//deps/rabbit:scripts/rabbitmq-streams",
            "//deps/rabbit:scripts/rabbitmq-upgrade",
            "//deps/rabbit:scripts/rabbitmqctl",
            "//deps/rabbit:scripts/vmware-rabbitmq",
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
            "//deps/rabbit:scripts/rabbitmq-streams.bat",
            "//deps/rabbit:scripts/rabbitmq-upgrade.bat",
            "//deps/rabbit:scripts/rabbitmqctl.bat",
            "//deps/rabbit:scripts/vmware-rabbitmq.bat",
        ],
        allow_files = True,
    ),
    "_rabbitmqctl_escript": attr.label(default = "//deps/rabbitmq_cli:rabbitmqctl"),
    "is_windows": attr.bool(mandatory = True),
    "plugins_dir": attr.label(
        mandatory = True,
        allow_single_file = [".tar"],
    ),
    # "compiled_applications": attr.label_list(
    #     providers = [CompileManyInfo],
    # ),
    # "plugins": attr.string_list(
    #     mandatory = True,
    # ),
    # "plugins": attr.label_list(providers = [ErlangAppInfo]),
}

rabbitmq_home_private = rule(
    implementation = _impl,
    attrs = RABBITMQ_HOME_ATTRS,
)

def rabbitmq_home(
        name = None,
        testonly = False,
        erl_libs = None,
        apps = None,
        visibility = ["//visibility:private"],
        **kwargs):
    extract_many_transitive(
        name = "%s-plugins" % name,
        testonly = testonly,
        erl_libs = erl_libs,
        apps = apps,
        out = "%s-plugins.tar" % name,
        visibility = visibility,
    )
    rabbitmq_home_private(
        name = name,
        testonly = testonly,
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        plugins_dir = ":%s-plugins" % name,
        visibility = visibility,
        **kwargs
    )

def _dirname(p):
    return p.rpartition("/")[0]

def rabbitmq_home_short_path(rabbitmq_home):
    short_path = rabbitmq_home[RabbitmqHomeInfo].rabbitmqctl.short_path
    if rabbitmq_home.label.workspace_root != "":
        short_path = path_join(rabbitmq_home.label.workspace_root, short_path)
    return _dirname(_dirname(short_path))
