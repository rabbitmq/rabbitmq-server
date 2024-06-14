load(
    "@rules_elixir//private:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)
load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)

def _impl(ctx):
    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx)

    app_info = ctx.attr.app[ErlangAppInfo]

    env = "\n".join([
        "export {}={}".format(k, v)
        for k, v in ctx.attr.env.items()
    ])

    config_path = ""
    if ctx.file.mix_config != None:
        config_path = ctx.file.mix_config.path

    command = """set -euo pipefail

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi

export OUT="{out}"
export CONFIG_PATH="{config_path}"
export APP="{app}"
export MAIN_MODULE="Elixir.{main_module}"

{env}

export PATH="{erlang_home}/bin:$PATH"
set -x
"{elixir_home}"/bin/elixir {script}
""".format(
        maybe_install_erlang = maybe_install_erlang(ctx),
        erlang_home = erlang_home,
        elixir_home = elixir_home,
        env = env,
        script = ctx.file._script.path,
        out = ctx.outputs.out.path,
        config_path = config_path,
        app = app_info.app_name,
        main_module = ctx.attr.main_module,
    )

    inputs = depset(
        direct = ctx.files._script + ctx.files.mix_config,
        transitive = [
            erlang_runfiles.files,
            elixir_runfiles.files,
        ],
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [ctx.outputs.out],
        command = command,
        mnemonic = "ELIXIR",
    )

elixir_escript_main = rule(
    implementation = _impl,
    attrs = {
        "_script": attr.label(
            allow_single_file = True,
            default = Label(":elixir_escript_main.exs"),
        ),
        "app": attr.label(
            providers = [ErlangAppInfo],
        ),
        "env": attr.string_dict(),
        "main_module": attr.string(),
        "mix_config": attr.label(
            allow_single_file = [".exs"],
        ),
        "out": attr.output(),
    },
    toolchains = [
        "@rules_elixir//:toolchain_type",
    ],
)
