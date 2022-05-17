load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_erlang//:erlang_app_info.bzl", "ErlangAppInfo", "flat_deps")
load("@rules_erlang//:util.bzl", "path_join")
load("@rules_erlang//:ct.bzl", "additional_file_dest_relative_path")
load(
    "@rules_erlang//tools:erlang_toolchain.bzl",
    "erlang_dirs",
    "maybe_install_erlang",
)
load(
    ":rabbitmq_home.bzl",
    "RABBITMQ_HOME_ATTRS",
    "RabbitmqHomeInfo",
    "flatten",
    "link_escript",
)
load(
    ":rabbitmq.bzl",
    "APP_VERSION",
)

def _collect_licenses_impl(ctx):
    srcs = ctx.files.srcs + flatten([
        d[ErlangAppInfo].license_files
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
        "deps": attr.label_list(providers = [ErlangAppInfo]),
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

    (erlang_home, _, runfiles) = erlang_dirs(ctx)

    inputs = runfiles.files.to_list()

    commands = [
        "set -euo pipefail",
        "",
        maybe_install_erlang(ctx),
    ]

    for plugin in plugins:
        lib_info = plugin[ErlangAppInfo]
        app_file = _app_file(lib_info)
        extract_version = _extract_version(app_file.path)
        commands.append("PLUGIN_VERSION=$({erlang_home}/bin/{extract_version})".format(erlang_home = erlang_home, extract_version = extract_version))

        commands.append(
            "echo \"Assembling {lib_name}-$PLUGIN_VERSION...\"".format(
                lib_name = lib_info.app_name,
            ),
        )

        commands.append(
            "mkdir -p {plugins_dir}/{lib_name}-$PLUGIN_VERSION/include".format(
                plugins_dir = plugins_dir.path,
                lib_name = lib_info.app_name,
            ),
        )
        for f in lib_info.include:
            commands.append(
                "cp {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION/include/{dest}".format(
                    src = f.path,
                    plugins_dir = plugins_dir.path,
                    lib_name = lib_info.app_name,
                    dest = f.basename,
                ),
            )
        inputs.extend(lib_info.include)

        commands.append(
            "mkdir -p {plugins_dir}/{lib_name}-$PLUGIN_VERSION/ebin".format(
                plugins_dir = plugins_dir.path,
                lib_name = lib_info.app_name,
            ),
        )
        for f in lib_info.beam:
            if f.is_directory:
                if f.basename != "ebin":
                    fail("{} contains a directory in 'beam' that is not an ebin dir".format(lib_info.app_name))
                commands.append(
                    "cp -R {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION".format(
                        src = f.path,
                        plugins_dir = plugins_dir.path,
                        lib_name = lib_info.app_name,
                    ),
                )
            else:
                commands.append(
                    "cp {src} {plugins_dir}/{lib_name}-$PLUGIN_VERSION/ebin/{dest}".format(
                        src = f.path,
                        plugins_dir = plugins_dir.path,
                        lib_name = lib_info.app_name,
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
                    lib_name = lib_info.app_name,
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

versioned_rabbitmq_home_private = rule(
    implementation = _versioned_rabbitmq_home_impl,
    attrs = RABBITMQ_HOME_ATTRS,
    toolchains = ["@rules_erlang//tools:toolchain_type"],
)

def versioned_rabbitmq_home(**kwargs):
    versioned_rabbitmq_home_private(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )

# This macro must be invoked from the top level BUILD.bazel of rabbitmq-server
def package_generic_unix(plugins):
    collect_licenses(
        name = "licenses",
        srcs = native.glob(
            ["LICENSE*"],
            exclude = [
                "LICENSE.md",
                "LICENSE.txt",
            ],
        ),
        deps = plugins,
    )

    pkg_tar(
        name = "license-files",
        srcs = [
            ":licenses",
            "//deps/rabbit:INSTALL",
        ],
        visibility = ["//visibility:public"],
    )

    pkg_tar(
        name = "scripts",
        srcs = [
            "scripts/bash_autocomplete.sh",
            "scripts/rabbitmq-script-wrapper",
            "scripts/rabbitmqctl-autocomplete.sh",
            "scripts/zsh_autocomplete.sh",
        ],
        package_dir = "scripts",
        visibility = ["//visibility:public"],
    )

    pkg_tar(
        name = "release-notes",
        srcs = native.glob([
            "release-notes/*.md",
            "release-notes/*.txt",
        ]),
        package_dir = "release-notes",
        visibility = ["//visibility:public"],
    )

    versioned_rabbitmq_home(
        name = "dist-home",
        plugins = plugins,
    )

    pkg_tar(
        name = "package-generic-unix",
        srcs = [
            ":dist-home",
        ],
        extension = "tar.xz",
        package_dir = "rabbitmq_server-{}".format(APP_VERSION),
        strip_prefix = "dist-home",
        visibility = ["//visibility:public"],
        deps = [
            ":license-files",
            ":release-notes",
            ":scripts",
            "//deps/rabbit:manpages-dir",
        ],
    )
