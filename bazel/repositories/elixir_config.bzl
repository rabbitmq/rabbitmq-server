load(
    "@rules_erlang//:util.bzl",
    "msys2_path",
    "path_join",
)

ELIXIR_HOME_ENV_VAR = "ELIXIR_HOME"

_DEFAULT_EXTERNAL_ELIXIR_PACKAGE_NAME = "external"
_ELIXIR_VERSION_UNKNOWN = "UNKNOWN"

INSTALLATION_TYPE_EXTERNAL = "external"
INSTALLATION_TYPE_INTERNAL = "internal"

def _version_identifier(version_string):
    parts = version_string.split(".", 2)
    if len(parts) > 1:
        return "{}_{}".format(parts[0], parts[1])
    else:
        return parts[0]

def _impl(repository_ctx):
    rabbitmq_server_workspace = repository_ctx.attr.rabbitmq_server_workspace

    elixir_installations = _default_elixir_dict(repository_ctx)
    for name in repository_ctx.attr.types.keys():
        if name == _DEFAULT_EXTERNAL_ELIXIR_PACKAGE_NAME:
            fail("'{}' is reserved as an elixir name".format(
                _DEFAULT_EXTERNAL_ELIXIR_PACKAGE_NAME,
            ))
        version = repository_ctx.attr.versions[name]
        identifier = _version_identifier(version)
        elixir_installations[name] = struct(
            type = repository_ctx.attr.types[name],
            version = version,
            identifier = identifier,
            url = repository_ctx.attr.urls.get(name, None),
            strip_prefix = repository_ctx.attr.strip_prefixs.get(name, None),
            sha256 = repository_ctx.attr.sha256s.get(name, None),
            elixir_home = repository_ctx.attr.elixir_homes.get(name, None),
        )

    for (name, props) in elixir_installations.items():
        if props.type == INSTALLATION_TYPE_EXTERNAL:
            repository_ctx.template(
                "{}/BUILD.bazel".format(name),
                Label("//bazel/repositories:BUILD_external.tpl"),
                {
                    "%{ELIXIR_HOME}": props.elixir_home,
                    "%{ELIXIR_VERSION_ID}": props.identifier,
                    "%{RABBITMQ_SERVER_WORKSPACE}": rabbitmq_server_workspace,
                },
                False,
            )
        else:
            repository_ctx.template(
                "{}/BUILD.bazel".format(name),
                Label("//bazel/repositories:BUILD_internal.tpl"),
                {
                    "%{URL}": props.url,
                    "%{STRIP_PREFIX}": props.strip_prefix or "",
                    "%{SHA_256}": props.sha256 or "",
                    "%{ELIXIR_VERSION_ID}": props.identifier,
                    "%{RABBITMQ_SERVER_WORKSPACE}": rabbitmq_server_workspace,
                },
                False,
            )

    if len(elixir_installations) == 0:
        fail("No elixir installations configured")

    repository_ctx.file(
        "BUILD.bazel",
        _build_file_content(elixir_installations),
        False,
    )

    toolchains = [
        "@{}//{}:toolchain".format(repository_ctx.name, name)
        for name in elixir_installations.keys()
    ]

    repository_ctx.template(
        "defaults.bzl",
        Label("//bazel/repositories:defaults.bzl.tpl"),
        {
            "%{TOOLCHAINS}": "\n".join([
                '        "%s",' % t
                for t in toolchains
            ]),
        },
        False,
    )

elixir_config = repository_rule(
    implementation = _impl,
    attrs = {
        "rabbitmq_server_workspace": attr.string(),
        "types": attr.string_dict(),
        "versions": attr.string_dict(),
        "urls": attr.string_dict(),
        "strip_prefixs": attr.string_dict(),
        "sha256s": attr.string_dict(),
        "elixir_homes": attr.string_dict(),
    },
    environ = [
        ELIXIR_HOME_ENV_VAR,
        "PATH",
    ],
    local = True,
)

def _elixir_home_from_elixir_path(repository_ctx, elixir_path):
    ehr = repository_ctx.execute(
        [
            elixir_path,
            "-e",
            "IO.puts Path.dirname(Path.dirname(Path.dirname(Path.dirname(Path.expand(:code.which(System))))))",
        ],
    )
    if ehr.return_code == 0:
        elixir_home = ehr.stdout.strip("\n")
    else:
        elixir_home = str(elixir_path.dirname.dirname)
    return elixir_home

def _is_windows(repository_ctx):
    return repository_ctx.os.name.lower().find("windows") != -1

def _default_elixir_dict(repository_ctx):
    if _is_windows(repository_ctx):
        if ELIXIR_HOME_ENV_VAR in repository_ctx.os.environ:
            elixir_home = repository_ctx.os.environ[ELIXIR_HOME_ENV_VAR]
            elixir_path = elixir_home + "\\bin\\elixir"
        else:
            elixir_path = repository_ctx.which("elixir")
            if elixir_path == None:
                elixir_path = repository_ctx.path("C:/Program Files (x86)/Elixir/bin/elixir")
            elixir_home = _elixir_home_from_elixir_path(repository_ctx, elixir_path)
        elixir_home = msys2_path(elixir_home)
    elif ELIXIR_HOME_ENV_VAR in repository_ctx.os.environ:
        elixir_home = repository_ctx.os.environ[ELIXIR_HOME_ENV_VAR]
        elixir_path = path_join(elixir_home, "bin", "elixir")
    else:
        elixir_path = repository_ctx.which("elixir")
        if elixir_path == None:
            elixir_path = repository_ctx.path("/usr/local/bin/elixir")
        elixir_home = _elixir_home_from_elixir_path(repository_ctx, elixir_path)

    version = repository_ctx.execute(
        [
            path_join(elixir_home, "bin", "elixir"),
            "-e",
            "IO.puts System.version()",
        ],
        timeout = 10,
    )
    if version.return_code == 0:
        version = version.stdout.strip("\n")
        identifier = _version_identifier(version)
        return {
            _DEFAULT_EXTERNAL_ELIXIR_PACKAGE_NAME: struct(
                type = INSTALLATION_TYPE_EXTERNAL,
                version = version,
                identifier = identifier,
                elixir_home = elixir_home,
            ),
        }
    else:
        return {
            _DEFAULT_EXTERNAL_ELIXIR_PACKAGE_NAME: struct(
                type = INSTALLATION_TYPE_EXTERNAL,
                version = _ELIXIR_VERSION_UNKNOWN,
                identifier = _ELIXIR_VERSION_UNKNOWN.lower(),
                elixir_home = elixir_home,
            ),
        }

def _build_file_content(elixir_installations):
    build_file_content = """\
package(
    default_visibility = ["//visibility:public"],
)

constraint_setting(
    name = "elixir_internal_external",
    default_constraint_value = ":elixir_external",
)

constraint_value(
    name = "elixir_external",
    constraint_setting = ":elixir_internal_external",
)

constraint_value(
    name = "elixir_internal",
    constraint_setting = ":elixir_internal_external",
)

"""

    default_installation = elixir_installations[_DEFAULT_EXTERNAL_ELIXIR_PACKAGE_NAME]

    build_file_content += """\
constraint_setting(
    name = "elixir_version",
    default_constraint_value = ":elixir_{}",
)

""".format(default_installation.identifier)

    unique_identifiers = {
        props.identifier: name
        for (name, props) in elixir_installations.items()
    }.keys()

    for identifier in unique_identifiers:
        build_file_content += """\
constraint_value(
    name = "elixir_{identifier}",
    constraint_setting = ":elixir_version",
)

platform(
    name = "elixir_{identifier}_platform",
    constraint_values = [
        ":elixir_{identifier}",
    ],
)

""".format(identifier = identifier)

    return build_file_content
