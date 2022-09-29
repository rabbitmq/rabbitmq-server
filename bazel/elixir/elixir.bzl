load(
    ":elixir_build.bzl",
    "elixir_build",
    "elixir_external",
)
load(
    ":elixir_toolchain.bzl",
    "elixir_toolchain",
)
load(
    "//bazel/repositories:elixir_config.bzl",
    "INSTALLATION_TYPE_INTERNAL",
    _elixir_config = "elixir_config",
)

def elixir_toolchain_external():
    """DEPRECATED"""

    elixir_external(
        name = "external_elixir_installation_ref",
        target_compatible_with = [
            Label("//bazel/platforms:elixir_external"),
        ],
    )

    elixir_toolchain(
        name = "elixir_external",
        elixir = ":external_elixir_installation_ref",
    )

    native.toolchain(
        name = "elixir_toolchain_external",
        exec_compatible_with = [
            Label("@erlang_config//:erlang_external"),
        ],
        target_compatible_with = [
            Label("//bazel/platforms:elixir_external"),
        ],
        toolchain = ":elixir_external",
        toolchain_type = Label("//bazel/elixir:toolchain_type"),
        visibility = ["//visibility:public"],
    )

def elixir_toolchain_from_http_archive(
        name_suffix = "",
        url = None,
        strip_prefix = None,
        sha256 = None,
        elixir_constraints = None):
    """DEPRECATED"""

    elixir_build(
        name = "elixir_build{}".format(name_suffix),
        url = url,
        strip_prefix = strip_prefix,
        sha256 = sha256,
        target_compatible_with = elixir_constraints,
    )

    elixir_toolchain(
        name = "elixir{}".format(name_suffix),
        elixir = ":elixir_build{}".format(name_suffix),
    )

    native.toolchain(
        name = "elixir_toolchain{}".format(name_suffix),
        exec_compatible_with = [
            Label("@erlang_config//:erlang_internal"),
        ],
        target_compatible_with = elixir_constraints,
        toolchain = ":elixir{}".format(name_suffix),
        toolchain_type = Label("//bazel/elixir:toolchain_type"),
        visibility = ["//visibility:public"],
    )

def elixir_toolchain_from_github_release(
        name_suffix = "_default",
        version = None,
        sha256 = None):
    """DEPRECATED"""

    [major, minor, patch] = version.split(".")
    elixir_constraints = [
        Label("//bazel/platforms:elixir_{}_{}".format(major, minor)),
    ]
    url = "https://github.com/elixir-lang/elixir/archive/refs/tags/v{}.tar.gz".format(version)
    elixir_toolchain_from_http_archive(
        name_suffix = name_suffix,
        url = url,
        strip_prefix = "elixir-{}".format(version),
        sha256 = sha256,
        elixir_constraints = elixir_constraints,
    )

DEFAULT_ELIXIR_VERSION = "1.13.4"
DEFAULT_ELIXIR_SHA256 = "95daf2dd3052e6ca7d4d849457eaaba09de52d65ca38d6933c65bc1cdf6b8579"

# Generates the @elixir_config repository, which contains erlang
# toolchains and platform defintions
def elixir_config(
        rabbitmq_server_workspace = "@rabbitmq-server",
        internal_elixir_configs = []):
    types = {c.name: INSTALLATION_TYPE_INTERNAL for c in internal_elixir_configs}
    versions = {c.name: c.version for c in internal_elixir_configs}
    urls = {c.name: c.url for c in internal_elixir_configs}
    strip_prefixs = {c.name: c.strip_prefix for c in internal_elixir_configs if c.strip_prefix}
    sha256s = {c.name: c.sha256 for c in internal_elixir_configs if c.sha256}

    _elixir_config(
        name = "elixir_config",
        rabbitmq_server_workspace = rabbitmq_server_workspace,
        types = types,
        versions = versions,
        urls = urls,
        strip_prefixs = strip_prefixs,
        sha256s = sha256s,
    )

def internal_elixir_from_http_archive(
        name = None,
        version = None,
        url = None,
        strip_prefix = None,
        sha256 = None):
    return struct(
        name = name,
        version = version,
        url = url,
        strip_prefix = strip_prefix,
        sha256 = sha256,
    )

def internal_elixir_from_github_release(
        name = "internal",
        version = DEFAULT_ELIXIR_VERSION,
        sha256 = DEFAULT_ELIXIR_SHA256):
    url = "https://github.com/elixir-lang/elixir/archive/refs/tags/v{}.tar.gz".format(
        version,
    )

    return internal_elixir_from_http_archive(
        name = name,
        version = version,
        url = url,
        strip_prefix = "elixir-{}".format(version),
        sha256 = sha256,
    )
