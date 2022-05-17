load(
    ":elixir_build.bzl",
    "elixir_build",
    "elixir_external",
)
load(
    ":elixir_toolchain.bzl",
    "elixir_toolchain",
)

def elixir_toolchain_external():
    elixir_constraint = Label("//bazel/platforms:elixir_external")

    elixir_external(
        name = "external_elixir_installation_ref",
        target_compatible_with = [
            elixir_constraint,
        ],
    )

    elixir_toolchain(
        name = "elixir_external",
        elixir = ":external_elixir_installation_ref",
    )

    native.toolchain(
        name = "elixir_toolchain_external",
        exec_compatible_with = [
            elixir_constraint,
        ],
        target_compatible_with = [
            elixir_constraint,
        ],
        toolchain = ":elixir_external",
        toolchain_type = Label("@rabbitmq-server//bazel/elixir:toolchain_type"),
        visibility = ["//visibility:public"],
    )

    return elixir_constraint

def elixir_toolchain_from_http_archive(
        name_suffix = "",
        version = None,
        url = None,
        strip_prefix = None,
        sha256 = None,
        elixir_constraint = None):
    elixir_build(
        name = "elixir_build{}".format(name_suffix),
        url = url,
        strip_prefix = strip_prefix,
        sha256 = sha256,
        target_compatible_with = [
            elixir_constraint,
        ],
    )

    elixir_toolchain(
        name = "elixir{}".format(name_suffix),
        elixir = ":elixir_build{}".format(name_suffix),
    )

    native.toolchain(
        name = "elixir_toolchain{}".format(name_suffix),
        exec_compatible_with = [
            elixir_constraint,
        ],
        target_compatible_with = [
            elixir_constraint,
        ],
        toolchain = ":elixir{}".format(name_suffix),
        toolchain_type = Label("@rabbitmq-server//bazel/elixir:toolchain_type"),
        visibility = ["//visibility:public"],
    )

def elixir_toolchain_from_github_release(
        name_suffix = "_default",
        version = None,
        sha256 = None):
    [major, minor, patch] = version.split(".")
    elixir_constraint = Label("@rabbitmq-server//bazel/platforms:elixir_{}_{}".format(major, minor))
    url = "https://github.com/elixir-lang/elixir/archive/refs/tags/v{}.tar.gz".format(version)
    elixir_toolchain_from_http_archive(
        name_suffix = name_suffix,
        url = url,
        strip_prefix = "elixir-{}".format(version),
        sha256 = sha256,
        elixir_constraint = elixir_constraint,
    )
    return elixir_constraint
