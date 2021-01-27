load(":erlang_home.bzl", "ErlangHomeProvider")

#TODO: go back to a place where we have an erlc rule that does compilation and emits
#      single files. Then have an additional erlang_lib rule that emits a directory,
#      either symlinking or coping the .hrl and .beam and .app files into a lib_parent
#      as this rule does. So it will be the erlang_lib rule that produces ErlangLibInfo
#      providers (The erlc rule will still consume ErlangLibInfos as deps). This way
#      it should be easier to have a hexpm rule and/or rebar3 rules (or even an
#      Erlang.mk rule) that also produce ErlangLibInfos, but we can also do incremental
#      compilation (both for compiling behaviors first) for the sources in the monorepo.
#      It would also be nice if there was a kind of beam CodePath provider that gave you
#      a path you could use with ERL_LIBS and was implemented by both the new erlang_lib
#      rule and the ez rule.
ErlangLibInfo = provider(
    doc = "Compiled Erlang sources",
    fields = {
        'lib_name': 'Name of the erlang lib',
        'lib_version': 'Version of the erlang lib',
        'lib_dir': 'A directory that contains the compiled lib',
        'erlang_version': 'The erlang version used to produce the beam files',
    },
)

_DEPS_DIR = "deps"

BEGINS_WITH_FUN = """beginswith() { case $2 in "$1"*) true;; *) false;; esac; }"""
QUERY_ERL_VERSION = """erl -eval '{ok, Version} = file:read_file(filename:join([code:root_dir(), "releases", erlang:system_info(otp_release), "OTP_VERSION"])), io:fwrite(Version), halt().' -noshell"""

# NOTE: we should probably fetch the separator with ctx.host_configuration.host_path_separator
def path_join(*components):
    return "/".join(components)

def ebin_dir(erlang_lib_info):
    return path_join(erlang_lib_info.lib_dir.path, "ebin")

def unique_dirnames(files):
    dirs = []
    for f in files:
        if f.dirname not in dirs:
            dirs.append(f.dirname)
    return dirs

def _module_name(f):
    return "'{}'".format(f.basename.replace(".erl", "", 1))

def _gen_app_file(ctx, srcs):
    app_file = ctx.actions.declare_file(
        path_join(
            ctx.label.name,
            "{}.app".format(ctx.attr.app_name),
        )
    )

    if len(ctx.files.app_src) > 1:
        fail("Multiple .app.src files ({}) are not supported".format(ctx.files.app_src))
    
    modules_list = "[" + ",".join([_module_name(src) for src in srcs]) + "]"

    if len(ctx.files.app_src) == 1:
        # print("Expanding {app_name}.app.src -> {app_name}.app and injecting modules list".format(app_name=ctx.attr.app_name))
        # TODO: check that the app_name in the .app.src matches the rule attribute
        #       as well as the version

        modules_term = "{modules," + modules_list + "}"

        # TODO: handle the data structure manipulation with erlang itself
        ctx.actions.expand_template(
            template = ctx.files.app_src[0],
            output = app_file,
            substitutions = {
                "{modules,[]}": modules_term,
            },
        )
    else:
        # print("Generating {app_name}.app".format(app_name=ctx.attr.app_name))
        if ctx.attr.app_module != "" and len([src for src in srcs if src.basename == ctx.attr.app_module + ".erl"]) == 1:
            template = ctx.file._app_with_mod_file_template
        else:
            template = ctx.file._app_file_template

        project_description = ctx.attr.app_description if ctx.attr.app_description != "" else ctx.attr.app_name

        registered_list = "[" + ",".join([ctx.attr.app_name + "_sup"] + ctx.attr.app_registered) + "]"

        # [$(call comma_list,kernel stdlib $(OTP_DEPS) $(LOCAL_DEPS) $(foreach dep,$(DEPS),$(call dep_name,$(dep))))]
        applications = ["kernel", "stdlib"] + ctx.attr.extra_apps
        for dep in ctx.attr.deps + ctx.attr.runtime_deps:
            applications.append(dep[ErlangLibInfo].lib_name)
        applications_list = "[" + ",".join(applications) + "]"

        ctx.actions.expand_template(
            template = template,
            output = app_file,
            substitutions = {
                "$(PROJECT)": ctx.attr.app_name,
                "$(PROJECT_DESCRIPTION)": project_description,
                "$(PROJECT_VERSION)": ctx.attr.app_version,
                "$(PROJECT_ID_TERM)": "", # {id$(comma)$(space)"$(1)"}$(comma))
                "$(MODULES_LIST)": modules_list,
                "$(REGISTERED_LIST)": registered_list,
                "$(APPLICATIONS_LIST)": applications_list,
                "$(PROJECT_MOD)": ctx.attr.app_module,
                "$(PROJECT_ENV)": ctx.attr.app_env, # $(subst \,\\,$(PROJECT_ENV))}$(if $(findstring {,$(PROJECT_APP_EXTRA_KEYS)),$(comma)$(newline)$(tab)$(subst \,\\,$(PROJECT_APP_EXTRA_KEYS)),)
            },
        )

    return app_file

def _deps_dir_link(ctx, dep):
    info = dep[ErlangLibInfo]
    output = ctx.actions.declare_file(
        path_join(
            "{}@{}".format(_DEPS_DIR, ctx.attr.erlang_version),
            info.lib_name,
        )
    )
    ctx.actions.symlink(
        output = output,
        target_file = info.lib_dir,
    )
    return output

def compile_erlang_action(ctx, srcs=[], hdrs=[], gen_app_file=True):
    app_file = _gen_app_file(ctx, srcs) if gen_app_file else None

    output_dir = ctx.actions.declare_directory(
        path_join(
            ctx.label.name,
            "{}-{}".format(ctx.attr.app_name, ctx.attr.app_version)
        )
    )

    # build a deps dir that can be passed erlc with -I
    deps_dir_contents = [_deps_dir_link(ctx, dep) for dep in ctx.attr.deps]

    dep_files = depset(transitive=[dep[DefaultInfo].files for dep in ctx.attr.deps])

    erl_args = ctx.actions.args()
    erl_args.add("-v")

    for dir in unique_dirnames(hdrs):
        erl_args.add("-I", dir)

    erl_args.add("-I",path_join(
        ctx.bin_dir.path,
        "{}@{}".format(_DEPS_DIR, ctx.attr.erlang_version),
    ))

    for dep in ctx.attr.deps:
        info = dep[ErlangLibInfo]
        if info.erlang_version != ctx.attr.erlang_version:
            fail("Mismatched erlang versions", ctx.attr.erlang_version, info.erlang_version)
        erl_args.add("-pa", ebin_dir(info))

    erl_args.add("-o", path_join(output_dir.path, "ebin"))

    erl_args.add_all(ctx.attr.erlc_opts)

    erl_args.add_all(srcs)

    expose_app_file_commands = []
    if app_file != None:
        expose_app_file_commands.append(
            "cp {app_file_path} {output_dir}/ebin/{app_name}.app".format(
                app_file_path = app_file.path,
                output_dir = output_dir.path,
                app_name = ctx.attr.app_name,
            )
        )

    expose_headers_commands = []
    for header in hdrs:
        src = header.path
        dst = path_join(output_dir.path, "include", header.basename)
        expose_headers_commands.append("cp {} {}".format(src, dst))

    expose_priv_commands = []
    for priv in ctx.files.priv:
        src = priv.path
        parts = priv.path.rpartition("priv/")
        dst = path_join(output_dir.path, "priv", parts[2])
        expose_priv_commands.append(
            "mkdir -p $(dirname {dst}) && cp {src} {dst}".format(src=src, dst=dst)
        )

    script = """
        set -euo pipefail

        mkdir -p {output_dir}
        mkdir -p {output_dir}/include
        mkdir -p {output_dir}/ebin
        mkdir -p {output_dir}/priv
        export HOME=$PWD

        {begins_with_fun}
        V=$({erlang_home}/bin/{query_erlang_version})
        if ! beginswith "{erlang_version}" "$V"; then
            echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
            exit 1
        fi

        {erlang_home}/bin/erlc $@
        {expose_app_file_command}
        {expose_headers_command}
        {expose_priv_command}
    """.format(
        output_dir=output_dir.path,
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_version=ctx.attr.erlang_version,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
        expose_app_file_command=" && ".join(expose_app_file_commands),
        expose_headers_command=" && ".join(expose_headers_commands),
        expose_priv_command=" && ".join(expose_priv_commands),
    )

    inputs = []
    inputs.extend(hdrs)
    inputs.extend(srcs)
    inputs.extend(ctx.files.priv)
    inputs.extend(deps_dir_contents)
    inputs.extend(dep_files.to_list())
    if app_file != None:
        inputs.append(app_file)

    # The script is computed differently based on the erlang version, so
    # why do we seem to get a cache collision?
    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [output_dir],
        command = script,
        arguments = [erl_args],
        env = {
            "ERLANG_VERSION": ctx.attr.erlang_version,
        },
    )

    return ErlangLibInfo(
        lib_name = ctx.attr.app_name,
        lib_version = ctx.attr.app_version,
        lib_dir = output_dir,
        erlang_version = ctx.attr.erlang_version,
    )

def _impl(ctx):
    erlang_lib_info = compile_erlang_action(ctx, srcs=ctx.files.srcs, hdrs=ctx.files.hdrs)

    return [
        DefaultInfo(files = depset([erlang_lib_info.lib_dir])),
        erlang_lib_info,
    ]

bazel_erlang_lib = rule(
    implementation = _impl,
    attrs = {
        "app_name": attr.string(mandatory=True),
        "app_version": attr.string(mandatory=True),
        "app_description": attr.string(),
        "app_module": attr.string(),
        "app_registered": attr.string_list(),
        "app_env": attr.string(default = "[]"),
        "extra_apps": attr.string_list(),
        "app_src": attr.label_list(allow_files=[".app.src"]), # type list > type optional
        "hdrs": attr.label_list(allow_files=[".hrl"]),
        "srcs": attr.label_list(allow_files=[".erl"]),
        "priv": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers=[ErlangLibInfo]),
        "runtime_deps": attr.label_list(providers=[ErlangLibInfo]),
        "erlc_opts": attr.string_list(),
        "erlang_version": attr.string(mandatory=True),
        "_erlang_home": attr.label(default = ":erlang_home"),
        "_app_file_template": attr.label(
            default = Label("//bazel_erlang:app_file.template"),
            allow_single_file = True,
        ),
        "_app_with_mod_file_template": attr.label(
            default = Label("//bazel_erlang:app_with_mod_file.template"),
            allow_single_file = True,
        ),
    },
)
