load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
    "flat_deps",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
)
load(
    "@rules_erlang//private:util.bzl",
    "additional_file_dest_relative_path",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)

ElixirAppInfo = provider(
    doc = "Compiled Elixir Application",
    fields = {
        "app_name": "Name of the erlang application",
        "extra_apps": "Extra applications in the applications key of the .app file",
        "include": "Public header files",
        "beam": "ebin directory produced by mix",
        "consolidated": "consolidated directory produced by mix",
        "priv": "Additional files",
        "license_files": "License files",
        "srcs": "Source files",
        "deps": "Runtime dependencies of the compiled sources",
    },
)

def deps_dir_contents(ctx, deps, dir):
    files = []
    for dep in deps:
        lib_info = dep[ErlangAppInfo]
        for src in lib_info.include + lib_info.beam + lib_info.srcs:
            if not src.is_directory:
                rp = additional_file_dest_relative_path(dep.label, src)
                f = ctx.actions.declare_file(path_join(
                    dir,
                    lib_info.app_name,
                    rp,
                ))
                ctx.actions.symlink(
                    output = f,
                    target_file = src,
                )
                files.extend([src, f])
    return files

def _impl(ctx):
    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    escript = ctx.actions.declare_file(path_join("escript", "rabbitmqctl"))
    ebin = ctx.actions.declare_directory("ebin")
    consolidated = ctx.actions.declare_directory("consolidated")
    mix_invocation_dir = ctx.actions.declare_directory("{}_mix".format(ctx.label.name))

    deps = flat_deps(ctx.attr.deps)

    deps_dir = ctx.label.name + "_deps"

    deps_dir_files = deps_dir_contents(ctx, deps, deps_dir)

    for dep, app_name in ctx.attr.source_deps.items():
        for src in dep.files.to_list():
            if not src.is_directory:
                rp = additional_file_dest_relative_path(dep.label, src)
                f = ctx.actions.declare_file(path_join(
                    deps_dir,
                    app_name,
                    rp,
                ))
                ctx.actions.symlink(
                    output = f,
                    target_file = src,
                )
                deps_dir_files.append(f)

    package_dir = path_join(
        ctx.label.workspace_root,
        ctx.label.package,
    )

    script = """set -euo pipefail

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi
ABS_EBIN_DIR=$PWD/{ebin_dir}
ABS_CONSOLIDATED_DIR=$PWD/{consolidated_dir}
ABS_ESCRIPT_PATH=$PWD/{escript_path}

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

MIX_INVOCATION_DIR="{mix_invocation_dir}"

cp -r {package_dir}/config ${{MIX_INVOCATION_DIR}}/config
cp -r {package_dir}/lib ${{MIX_INVOCATION_DIR}}/lib
cp    {package_dir}/mix.exs ${{MIX_INVOCATION_DIR}}/mix.exs

ORIGINAL_DIR=$PWD
cd ${{MIX_INVOCATION_DIR}}
export IS_BAZEL=true
export HOME=${{PWD}}
export DEPS_DIR=$(dirname $ABS_EBIN_DIR)/{deps_dir}
export MIX_ENV=prod
export ERL_COMPILER_OPTIONS=deterministic
for archive in {archives}; do
    "${{ABS_ELIXIR_HOME}}"/bin/mix archive.install --force $ORIGINAL_DIR/$archive
done
for d in {precompiled_deps}; do
    mkdir -p _build/${{MIX_ENV}}/lib/$d
    ln -s ${{DEPS_DIR}}/$d/ebin _build/${{MIX_ENV}}/lib/$d
    ln -s ${{DEPS_DIR}}/$d/include _build/${{MIX_ENV}}/lib/$d
done
"${{ABS_ELIXIR_HOME}}"/bin/mix deps.compile
"${{ABS_ELIXIR_HOME}}"/bin/mix compile
"${{ABS_ELIXIR_HOME}}"/bin/mix escript.build

cp escript/rabbitmqctl ${{ABS_ESCRIPT_PATH}}

cp _build/${{MIX_ENV}}/lib/rabbitmqctl/ebin/* ${{ABS_EBIN_DIR}}
cp _build/${{MIX_ENV}}/lib/rabbitmqctl/consolidated/* ${{ABS_CONSOLIDATED_DIR}}

# remove symlinks from the _build directory since it
# is not used, and bazel does not allow them
find . -type l -delete
""".format(
        maybe_install_erlang = maybe_install_erlang(ctx),
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        mix_invocation_dir = mix_invocation_dir.path,
        package_dir = package_dir,
        deps_dir = deps_dir,
        escript_path = escript.path,
        ebin_dir = ebin.path,
        consolidated_dir = consolidated.path,
        archives = "".join([a.path for a in ctx.files.archives]),
        precompiled_deps = " ".join([
            dep[ErlangAppInfo].app_name
            for dep in ctx.attr.deps
        ]),
    )

    inputs = depset(
        direct = ctx.files.srcs,
        transitive = [
            erlang_runfiles.files,
            elixir_runfiles.files,
            depset(ctx.files.archives),
            depset(deps_dir_files),
        ],
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [
            escript,
            ebin,
            consolidated,
            mix_invocation_dir,
        ],
        command = script,
        mnemonic = "MIX",
    )

    runfiles = ctx.runfiles([ebin, consolidated]).merge_all([
        erlang_runfiles,
        elixir_runfiles,
    ] + [
        dep[DefaultInfo].default_runfiles
        for dep in deps
    ])

    return [
        DefaultInfo(
            executable = escript,
            files = depset([ebin, consolidated]),
            runfiles = runfiles,
        ),
        ElixirAppInfo(
            app_name = "rabbitmqctl",  # mix generates 'rabbitmqctl.app'
            extra_apps = ["elixir", "logger"],
            include = [],
            beam = ebin,
            consolidated = consolidated,
            priv = [],
            license_files = ctx.files.license_files,
            srcs = ctx.files.srcs,
            deps = deps,
        ),
    ]

rabbitmqctl_private = rule(
    implementation = _impl,
    attrs = {
        "is_windows": attr.bool(
            mandatory = True,
        ),
        "srcs": attr.label_list(
            mandatory = True,
            allow_files = True,
        ),
        "license_files": attr.label_list(
            allow_files = True,
        ),
        "deps": attr.label_list(
            providers = [ErlangAppInfo],
        ),
        "archives": attr.label_list(
            allow_files = [".ez"],
        ),
        "source_deps": attr.label_keyed_string_dict(),
    },
    toolchains = [
        "//bazel/elixir:toolchain_type",
    ],
    provides = [ElixirAppInfo],
    executable = True,
)

def _elixir_app_to_erlang_app(ctx):
    app_consolidated = ctx.attr.elixir_app[ElixirAppInfo].consolidated
    app_ebin = ctx.attr.elixir_app[ElixirAppInfo].beam

    elixir_ebin = ctx.attr.elixir_as_app[ErlangAppInfo].beam[0].path

    ebin = ctx.actions.declare_directory(path_join(ctx.label.name, "ebin"))

    if ctx.attr.mode == "elixir":
        ctx.actions.run_shell(
            inputs = ctx.files.elixir_as_app + ctx.files.elixir_app,
            outputs = [ebin],
            command = """\
set -euo pipefail

cp "{elixir_ebin}"/* "{ebin}"

for beam in "{app_consolidated}"/*; do
    find "{ebin}" -name "$(basename $beam)" -exec cp -f "$beam" "{ebin}" \\;
done
""".format(
                elixir_ebin = elixir_ebin,
                app_consolidated = app_consolidated.path,
                ebin = ebin.path,
            ),
        )

        lib_info = ctx.attr.elixir_as_app[ErlangAppInfo]
        return [
            DefaultInfo(files = depset([ebin])),
            ErlangAppInfo(
                app_name = "elixir",
                include = lib_info.include,
                beam = [ebin],
                priv = lib_info.priv,
                deps = lib_info.deps,
            ),
        ]
    elif ctx.attr.mode == "app":
        ctx.actions.run_shell(
            inputs = ctx.files.elixir_as_app + ctx.files.elixir_app,
            outputs = [ebin],
            command = """\
set -euo pipefail

cp "{app_ebin}"/* "{ebin}"
cp -f "{app_consolidated}"/* "{ebin}"

for beam in "{elixir_ebin}"/*; do
    find "{ebin}" -name "$(basename $beam)" -delete
done
""".format(
                elixir_ebin = elixir_ebin,
                app_ebin = app_ebin.path,
                app_consolidated = app_consolidated.path,
                ebin = ebin.path,
            ),
        )

        (_, _, erlang_runfiles) = erlang_dirs(ctx)
        (_, elixir_runfiles) = elixir_dirs(ctx)

        lib_info = ctx.attr.elixir_app[ElixirAppInfo]

        runfiles = ctx.runfiles([ebin]).merge_all([
            erlang_runfiles,
            elixir_runfiles,
        ] + [
            dep[DefaultInfo].default_runfiles
            for dep in lib_info.deps
        ])

        return [
            DefaultInfo(
                files = depset([ebin]),
                runfiles = runfiles,
            ),
            ErlangAppInfo(
                app_name = lib_info.app_name,
                extra_apps = lib_info.extra_apps,
                include = lib_info.include,
                beam = [ebin],
                priv = lib_info.priv,
                license_files = lib_info.license_files,
                srcs = lib_info.srcs,
                deps = lib_info.deps,
            ),
        ]

    return []

elixir_app_to_erlang_app = rule(
    implementation = _elixir_app_to_erlang_app,
    attrs = {
        "elixir_as_app": attr.label(
            providers = [ErlangAppInfo],
        ),
        "elixir_app": attr.label(
            providers = [ElixirAppInfo],
        ),
        "mode": attr.string(
            values = [
                "elixir",
                "app",
            ],
        ),
    },
    toolchains = [
        "//bazel/elixir:toolchain_type",
    ],
    provides = [ErlangAppInfo],
)

def rabbitmqctl(
        name = None,
        visibility = None,
        **kwargs):
    # mix produces a consolidated directory alongside the ebin
    # directory, which contains .beam files for modules that
    # are extended by protocols
    # When used with dialyzer, this results in module conflicts
    # between the original versions in elixir, and the
    # consolidated ones
    # So, this macro compiles the cli, then derives a copy of
    # elixir that can be loaded alongside it without conflict
    # (but assumes that the two are used together)
    # These each have to be separate rules, as a single rule
    # cannot provide multiple erlang_app (ErlangAppInfo
    # provider instances)

    rabbitmqctl_private(
        name = name,
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        visibility = visibility,
        **kwargs
    )

    elixir_app_to_erlang_app(
        name = "elixir",
        elixir_as_app = Label("//bazel/elixir:erlang_app"),
        elixir_app = ":" + name,
        mode = "elixir",
        visibility = visibility,
    )

    elixir_app_to_erlang_app(
        name = "erlang_app",
        elixir_as_app = Label("//bazel/elixir:erlang_app"),
        elixir_app = ":" + name,
        mode = "app",
        visibility = visibility,
    )
