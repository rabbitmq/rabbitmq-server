load(
    "@rules_erlang//:util.bzl",
    "path_join",
    "windows_path",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)

def _impl(ctx):
    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx, short_path = True)

    package_dir = path_join(
        ctx.label.workspace_root,
        ctx.label.package,
    )

    if not ctx.attr.is_windows:
        output = ctx.actions.declare_file(ctx.label.name)
        script = """set -euo pipefail

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

INITIAL_DIR="$(pwd)"

if [ ! -f ${{INITIAL_DIR}}/{package_dir}/test/test_helper.exs ]; then
    echo "test_helper.exs cannot be found. 'bazel clean' might fix this."
    exit 1
fi

cp -r ${{INITIAL_DIR}}/{package_dir}/config ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp -r ${{INITIAL_DIR}}/{package_dir}/lib ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp -r ${{INITIAL_DIR}}/{package_dir}/test ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp    ${{INITIAL_DIR}}/{package_dir}/mix.exs ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp    ${{INITIAL_DIR}}/{package_dir}/.formatter.exs ${{TEST_UNDECLARED_OUTPUTS_DIR}}

cd ${{TEST_UNDECLARED_OUTPUTS_DIR}}

export IS_BAZEL=true
export HOME=${{PWD}}
export MIX_ENV=test
export ERL_COMPILER_OPTIONS=deterministic
set -x
"${{ABS_ELIXIR_HOME}}"/bin/mix format --check-formatted
""".format(
            maybe_install_erlang = maybe_install_erlang(ctx, short_path = True),
            erlang_home = erlang_home,
            elixir_home = elixir_home,
            package_dir = package_dir,
        )
    else:
        output = ctx.actions.declare_file(ctx.label.name + ".bat")
        script = """@echo off
:: set LANG="en_US.UTF-8"
:: set LC_ALL="en_US.UTF-8"

set PATH="{elixir_home}\\bin";"{erlang_home}\\bin";%PATH%

set OUTPUTS_DIR=%TEST_UNDECLARED_OUTPUTS_DIR:/=\\%

:: robocopy exits non-zero when files are copied successfully
:: https://social.msdn.microsoft.com/Forums/en-US/d599833c-dcea-46f5-85e9-b1f028a0fefe/robocopy-exits-with-error-code-1?forum=tfsbuild
robocopy {package_dir}\\config %OUTPUTS_DIR%\\config /E /NFL /NDL /NJH /NJS /nc /ns /np
robocopy {package_dir}\\lib %OUTPUTS_DIR%\\lib /E /NFL /NDL /NJH /NJS /nc /ns /np
robocopy {package_dir}\\test %OUTPUTS_DIR%\\test /E /NFL /NDL /NJH /NJS /nc /ns /np
copy {package_dir}\\mix.exs %OUTPUTS_DIR%\\mix.exs || goto :error
copy {package_dir}\\.formatter.exs %OUTPUTS_DIR%\\.formatter.exs || goto :error

cd %OUTPUTS_DIR% || goto :error

set ERL_COMPILER_OPTIONS=deterministic
set MIX_ENV=test
"{elixir_home}\\bin\\mix" format --check-formatted || goto :error
goto :EOF
:error
exit /b 1
""".format(
            erlang_home = windows_path(erlang_home),
            elixir_home = windows_path(elixir_home),
            package_dir = windows_path(ctx.label.package),
        )

    ctx.actions.write(
        output = output,
        content = script,
    )

    runfiles = ctx.runfiles(
        files = ctx.files.srcs + ctx.files.data,
    ).merge_all([
        erlang_runfiles,
        elixir_runfiles,
    ])

    return [DefaultInfo(
        runfiles = runfiles,
        executable = output,
    )]

rabbitmqctl_check_formatted_private_test = rule(
    implementation = _impl,
    attrs = {
        "is_windows": attr.bool(mandatory = True),
        "srcs": attr.label_list(allow_files = [".ex", ".exs"]),
        "data": attr.label_list(allow_files = True),
    },
    toolchains = [
        "//bazel/elixir:toolchain_type",
    ],
    test = True,
)

def rabbitmqctl_check_formatted_test(**kwargs):
    rabbitmqctl_check_formatted_private_test(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )
