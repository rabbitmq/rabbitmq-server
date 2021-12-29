load("@bazel-erlang//:erlang_home.bzl", "ErlangHomeProvider")
load("@bazel-erlang//:bazel_erlang_lib.bzl", "ErlangLibInfo", "flat_deps", "path_join")
load("@bazel-erlang//:ct.bzl", "additional_file_dest_relative_path")
load(
    ":rabbitmq_home.bzl",
    "RABBITMQ_HOME_ATTRS",
    "RabbitmqHomeInfo",
    "flatten",
    "link_escript",
    "unique_versions",
)

def _collect_licenses_impl(ctx):
    srcs = ctx.files.srcs + flatten([
        d[ErlangLibInfo].license_files
        for d in flat_deps(ctx.attr.deps)
    ])

    outs = {}
    for src in srcs:
        name = src.basename
        if name not in outs:
            dest = ctx.actions.declare_file(name)
            ctx.actions.run(
                inputs = [src],
                outputs = [dest],
                executable = "cp",
                arguments = [
                    src.path,
                    dest.path,
                ],
            )
            outs[name] = dest

    return [
        DefaultInfo(
            files = depset(sorted(outs.values())),
        ),
    ]

collect_licenses = rule(
    implementation = _collect_licenses_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ErlangLibInfo]),
    },
)

def _copy_script(ctx, script):
    dest = ctx.actions.declare_file(path_join(ctx.label.name, "sbin", script.basename))
    ctx.actions.expand_template(
        template = script,
        output = dest,
        substitutions = {
            "SYS_PREFIX=": "SYS_PREFIX=${RABBITMQ_HOME}",
        },
    )
    return dest

def _extract_version(p):
    return "erl -eval '{ok, [{application, _, AppInfo}]} = file:consult(\"" + p + "\"), Version = proplists:get_value(vsn, AppInfo), io:fwrite(Version), halt().' -noshell"

def _app_file(plugin_lib_info):
    for f in plugin_lib_info.beam:
        if f.basename.endswith(".app"):
            return f
    fail(".app file not found in {}".format(plugin_lib_info))

def _plugins_dir(ctx, plugins):
    plugins_dir = ctx.actions.declare_directory(path_join(ctx.label.name, "plugins"))

    erlang_home = ctx.attr._erlang_home[ErlangHomeProvider].path

    inputs = []

    commands = ["set -euo pipefail", ""]

    for plugin in plugins:
        lib_info = plugin[ErlangLibInfo]
        app_file = _app_file(lib_info)
        extract_version = _extract_version(app_file.path)
        commands.append("PLUGIN_VERSION=$({erlang_home}/bin/{extract_version})".format(erlang_home = erlang_home, extract_version = extract_version))

        commands.append(
            "echo \"Assembling {lib_name}-$PLUGIN_VERSION...\"".format(
                lib_name = lib_info.lib_name,
            ),
        )

        commands.append(
            "mkdir -p {plugins_dir}/{lib_name}-$PLUGIN_VERSION/include".format(
                plugins_dir = plugins_dir.path,
                lib_name = lib_info.lib_name,
            ),
        )
        for f in lib_info.include:
            commands.append(
                "cp {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION/include/{dest}".format(
                    src = f.path,
                    plugins_dir = plugins_dir.path,
                    lib_name = lib_info.lib_name,
                    dest = f.basename,
                ),
            )
        inputs.extend(lib_info.include)

        commands.append(
            "mkdir -p {plugins_dir}/{lib_name}-$PLUGIN_VERSION/ebin".format(
                plugins_dir = plugins_dir.path,
                lib_name = lib_info.lib_name,
            ),
        )
        for f in lib_info.beam:
            if f.is_directory:
                if f.basename != "ebin":
                    fail("{} contains a directory in 'beam' that is not an ebin dir".format(lib_info.lib_name))
                commands.append(
                    "cp -R {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION".format(
                        src = f.path,
                        plugins_dir = plugins_dir.path,
                        lib_name = lib_info.lib_name,
                    ),
                )
            else:
                commands.append(
                    "cp {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION/ebin/{dest}".format(
                        src = f.path,
                        plugins_dir = plugins_dir.path,
                        lib_name = lib_info.lib_name,
                        dest = f.basename,
                    ),
                )
        inputs.extend(lib_info.beam)

        for f in lib_info.priv:
            p = additional_file_dest_relative_path(plugin.label, f)
            commands.append(
                "mkdir -p $(dirname {plugins_dir}/{lib_name}-$PLUGIN_VERSION/{dest}) && cp {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION/{dest}".format(
                    src = f.path,
                    plugins_dir = plugins_dir.path,
                    lib_name = lib_info.lib_name,
                    dest = p,
                ),
            )
        inputs.extend(lib_info.priv)

        commands.append("")

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [plugins_dir],
        command = "\n".join(commands),
    )

    return plugins_dir

def _versioned_rabbitmq_home_impl(ctx):
    plugins = flat_deps(ctx.attr.plugins)

    erlang_versions = unique_versions(plugins)
    if len(erlang_versions) > 1:
        fail("plugins do not have a unified erlang version", erlang_versions)

    scripts = [_copy_script(ctx, script) for script in ctx.files._scripts]

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

    plugins_dir = _plugins_dir(ctx, plugins)

    rabbitmqctl = None
    for script in scripts:
        if script.basename == "rabbitmqctl":
            rabbitmqctl = script
    if rabbitmqctl == None:
        fail("could not find rabbitmqct among", scripts)

    return [
        RabbitmqHomeInfo(
            rabbitmqctl = rabbitmqctl,
        ),
        DefaultInfo(
            files = depset(scripts + escripts + [plugins_dir]),
        ),
    ]

versioned_rabbitmq_home = rule(
    implementation = _versioned_rabbitmq_home_impl,
    attrs = dict(RABBITMQ_HOME_ATTRS.items() + {
        "_erlang_home": attr.label(default = "@bazel-erlang//:erlang_home"),
    }.items()),
)
