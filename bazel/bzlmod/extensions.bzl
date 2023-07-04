load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load(
    ":secondary_umbrella.bzl",
    fetch_secondary_umbrella = "secondary_umbrella",
)
load(
    "//bazel/repositories:elixir_config.bzl",
    "INSTALLATION_TYPE_EXTERNAL",
    "INSTALLATION_TYPE_INTERNAL",
    _elixir_config_rule = "elixir_config",
)
load(
    "//bazel/elixir:elixir.bzl",
    "DEFAULT_ELIXIR_SHA256",
    "DEFAULT_ELIXIR_VERSION",
)

def _elixir_config(ctx):
    types = {}
    versions = {}
    urls = {}
    strip_prefixs = {}
    sha256s = {}
    elixir_homes = {}

    for mod in ctx.modules:
        for elixir in mod.tags.external_elixir_from_path:
            types[elixir.name] = INSTALLATION_TYPE_EXTERNAL
            versions[elixir.name] = elixir.version
            elixir_homes[elixir.name] = elixir.elixir_home

        for elixir in mod.tags.internal_elixir_from_http_archive:
            types[elixir.name] = INSTALLATION_TYPE_INTERNAL
            versions[elixir.name] = elixir.version
            urls[elixir.name] = elixir.url
            strip_prefixs[elixir.name] = elixir.strip_prefix
            sha256s[elixir.name] = elixir.sha256

        for elixir in mod.tags.internal_elixir_from_github_release:
            url = "https://github.com/elixir-lang/elixir/archive/refs/tags/v{}.tar.gz".format(
                elixir.version,
            )
            strip_prefix = "elixir-{}".format(elixir.version)

            types[elixir.name] = INSTALLATION_TYPE_INTERNAL
            versions[elixir.name] = elixir.version
            urls[elixir.name] = url
            strip_prefixs[elixir.name] = strip_prefix
            sha256s[elixir.name] = elixir.sha256

    _elixir_config_rule(
        name = "elixir_config",
        rabbitmq_server_workspace = "@rabbitmq-server",
        types = types,
        versions = versions,
        urls = urls,
        strip_prefixs = strip_prefixs,
        sha256s = sha256s,
        elixir_homes = elixir_homes,
    )

external_elixir_from_path = tag_class(attrs = {
    "name": attr.string(),
    "version": attr.string(),
    "elixir_home": attr.string(),
})

internal_elixir_from_http_archive = tag_class(attrs = {
    "name": attr.string(),
    "version": attr.string(),
    "url": attr.string(),
    "strip_prefix": attr.string(),
    "sha256": attr.string(),
})

internal_elixir_from_github_release = tag_class(attrs = {
    "name": attr.string(
        default = "internal",
    ),
    "version": attr.string(
        default = DEFAULT_ELIXIR_VERSION,
    ),
    "sha256": attr.string(
        default = DEFAULT_ELIXIR_SHA256,
    ),
})

elixir_config = module_extension(
    implementation = _elixir_config,
    tag_classes = {
        "external_elixir_from_path": external_elixir_from_path,
        "internal_elixir_from_http_archive": internal_elixir_from_http_archive,
        "internal_elixir_from_github_release": internal_elixir_from_github_release,
    },
)

def _rbe(ctx):
    root_rbe_repo_props = []
    rbe_repo_props = []
    for mod in ctx.modules:
        for repo in mod.tags.git_repository:
            props = {"remote": repo.remote}
            if repo.commit != "":
                props["commit"] = repo.commit
            if repo.tag != "":
                props["tag"] = repo.tag
            if repo.branch != "":
                props["branch"] = repo.branch
            if mod.is_root:
                if not props in root_rbe_repo_props:
                    root_rbe_repo_props.append(props)
            elif not props in rbe_repo_props:
                rbe_repo_props.append(props)

    if len(root_rbe_repo_props) > 1:
        fail("Multiple definitions for @rbe exist in root module: {}".format(rbe_repo_props))

    if len(root_rbe_repo_props) > 0:
        git_repository(
            name = "rbe",
            **root_rbe_repo_props[0]
        )
    else:
        if len(rbe_repo_props) > 1:
            fail("Multiple definitions for @rbe exist: {}".format(rbe_repo_props))

        if len(rbe_repo_props) > 0:
            git_repository(
                name = "rbe",
                **rbe_repo_props[0]
            )

git_repository_tag = tag_class(attrs = {
    "remote": attr.string(),
    "branch": attr.string(),
    "tag": attr.string(),
    "commit": attr.string(),
})

rbe = module_extension(
    implementation = _rbe,
    tag_classes = {
        "git_repository": git_repository_tag,
    },
)

def _secondary_umbrella(ctx):
    fetch_secondary_umbrella()

secondary_umbrella = module_extension(
    implementation = _secondary_umbrella,
)

def _hex(ctx):
    http_archive(
        name = "hex",
        sha256 = "0e3e3290d0fcbdc6bb0526b73ca174d68dcff4d53ee86015c49ad0493e39ee65",
        strip_prefix = "hex-2.0.5",
        urls = ["https://github.com/hexpm/hex/archive/refs/tags/v2.0.5.zip"],
        build_file_content = """\
load(
    "@rabbitmq-server//bazel/elixir:mix_archive_build.bzl",
    "mix_archive_build",
)

mix_archive_build(
    name = "archive",
    srcs = [
        "mix.exs",
    ] + glob([
        "lib/**/*",
    ]),
    out = "hex.ez",
    visibility = ["//visibility:public"],
)
""",
    )

hex = module_extension(
    implementation = _hex,
)
