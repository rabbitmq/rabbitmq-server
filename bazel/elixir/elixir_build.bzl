load(
    "@bazel_skylib//rules:common_settings.bzl",
    "BuildSettingInfo",
)
load(
    "@rules_erlang//tools:erlang_toolchain.bzl",
    "erlang_dirs",
    "maybe_install_erlang",
)

ElixirInfo = provider(
    doc = "A Home directory of a built Elixir",
    fields = [
        "release_dir",
        "elixir_home",
        "version_file",
    ],
)

def _impl(ctx):
    (_, _, filename) = ctx.attr.url.rpartition("/")
    downloaded_archive = ctx.actions.declare_file(filename)

    release_dir = ctx.actions.declare_directory(ctx.label.name + "_release")
    build_dir = ctx.actions.declare_directory(ctx.label.name + "_build")

    version_file = ctx.actions.declare_file(ctx.label.name + "_version")

    ctx.actions.run_shell(
        inputs = [],
        outputs = [downloaded_archive],
        command = """set -euo pipefail

curl -L "{archive_url}" -o {archive_path}

if [ -n "{sha256}" ]; then
    echo "{sha256} {archive_path}" | sha256sum --check --strict -
fi
""".format(
            archive_url = ctx.attr.url,
            archive_path = downloaded_archive.path,
            sha256 = ctx.attr.sha256,
        ),
        mnemonic = "CURL",
        progress_message = "Downloading {}".format(ctx.attr.url),
    )

    (erlang_home, _, runfiles) = erlang_dirs(ctx)

    inputs = depset(
        direct = [downloaded_archive],
        transitive = [runfiles.files],
    )

    strip_prefix = ctx.attr.strip_prefix
    if strip_prefix != "":
        strip_prefix += "\\/"

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [release_dir, build_dir, version_file],
        command = """set -euo pipefail

{maybe_install_erlang}

export PATH="{erlang_home}"/bin:${{PATH}}

ABS_BUILD_DIR=$PWD/{build_path}
ABS_RELEASE_DIR=$PWD/{release_path}
ABS_VERSION_FILE=$PWD/{version_file}

tar --extract \\
    --transform 's/{strip_prefix}//' \\
    --file {archive_path} \\
    --directory $ABS_BUILD_DIR

cd $ABS_BUILD_DIR

make

cp -r bin $ABS_RELEASE_DIR/
cp -r lib $ABS_RELEASE_DIR/

$ABS_RELEASE_DIR/bin/iex --version > $ABS_VERSION_FILE
""".format(
            maybe_install_erlang = maybe_install_erlang(ctx),
            erlang_home = erlang_home,
            archive_path = downloaded_archive.path,
            strip_prefix = strip_prefix,
            build_path = build_dir.path,
            release_path = release_dir.path,
            version_file = version_file.path,
        ),
        mnemonic = "ELIXIR",
        progress_message = "Compiling elixir from source",
    )

    return [
        DefaultInfo(
            files = depset([
                release_dir,
                version_file,
            ]),
        ),
        ctx.toolchains["@rules_erlang//tools:toolchain_type"].otpinfo,
        ElixirInfo(
            release_dir = release_dir,
            elixir_home = None,
            version_file = version_file,
        ),
    ]

elixir_build = rule(
    implementation = _impl,
    attrs = {
        "url": attr.string(mandatory = True),
        "strip_prefix": attr.string(),
        "sha256": attr.string(),
    },
    toolchains = ["@rules_erlang//tools:toolchain_type"],
)

def _elixir_external_impl(ctx):
    elixir_home = ctx.attr._elixir_home[BuildSettingInfo].value

    version_file = ctx.actions.declare_file(ctx.label.name + "_version")

    ctx.actions.run_shell(
        inputs = [],
        outputs = [version_file],
        command = """set -euo pipefail

"{elixir_home}"/bin/iex --version > {version_file}
""".format(
            elixir_home = elixir_home,
            version_file = version_file.path,
        ),
        mnemonic = "ELIXIR",
        progress_message = "Validating elixir at {}".format(elixir_home),
    )

    return [
        DefaultInfo(
            files = depset([version_file]),
        ),
        ctx.toolchains["@rules_erlang//tools:toolchain_type"].otpinfo,
        ElixirInfo(
            release_dir = None,
            elixir_home = elixir_home,
            version_file = version_file,
        ),
    ]

elixir_external = rule(
    implementation = _elixir_external_impl,
    attrs = {
        "_elixir_home": attr.label(default = Label("//:elixir_home")),
    },
    toolchains = ["@rules_erlang//tools:toolchain_type"],
)
