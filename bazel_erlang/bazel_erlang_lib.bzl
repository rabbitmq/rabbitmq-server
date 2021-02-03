load(":erlang_home.bzl", "ErlangHomeProvider", "ErlangVersionProvider")

ErlangLibInfo = provider(
    doc = "Compiled Erlang sources",
    fields = {
        'lib_name': 'Name of the erlang lib',
        'lib_version': 'Version of the erlang lib',
        'erlang_version': 'The erlang version used to produce the beam files',
        'include': 'Public header files',
        'beam': 'Compiled bytecode',
        'priv': 'Additional files',
    },
)

_DEPS_DIR = "deps"

BEGINS_WITH_FUN = """beginswith() { case $2 in "$1"*) true;; *) false;; esac; }"""
QUERY_ERL_VERSION = """erl -eval '{ok, Version} = file:read_file(filename:join([code:root_dir(), "releases", erlang:system_info(otp_release), "OTP_VERSION"])), io:fwrite(Version), halt().' -noshell"""

# NOTE: we should probably fetch the separator with ctx.host_configuration.host_path_separator
def path_join(*components):
    return "/".join(components)

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
        path_join("ebin", "{}.app".format(ctx.attr.app_name))
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

def _beam_file(ctx, src):
    name = src.basename.replace(".erl", ".beam")
    return ctx.actions.declare_file(path_join("ebin", name))

def compile_erlang_action(ctx, srcs=[], hdrs=[]):
    erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version

    beam_files = [_beam_file(ctx, src) for src in ctx.files.srcs]

    ebin_dir = beam_files[0].dirname

    erl_args = ctx.actions.args()
    erl_args.add("-v")

    for dir in unique_dirnames(hdrs):
        erl_args.add("-I", dir)

    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        if lib_info.erlang_version != erlang_version:
            fail("Mismatched erlang versions", erlang_version, lib_info.erlang_version)
        for dir in unique_dirnames(lib_info.include):
            erl_args.add("-I", path_join(dir, "../.."))
        for dir in unique_dirnames(lib_info.beam):
            erl_args.add("-pa", dir)

    erl_args.add("-o", ebin_dir)

    erl_args.add_all(ctx.attr.erlc_opts)

    erl_args.add_all(srcs)

    script = """
        set -euo pipefail

        # /usr/local/bin/tree $PWD

        mkdir -p {ebin_dir}
        export HOME=$PWD

        {begins_with_fun}
        V=$({erlang_home}/bin/{query_erlang_version})
        if ! beginswith "{erlang_version}" "$V"; then
            echo "Erlang version mismatch (Expected {erlang_version}, found $V)"
            exit 1
        fi

        {erlang_home}/bin/erlc $@
    """.format(
        ebin_dir=ebin_dir,
        begins_with_fun=BEGINS_WITH_FUN,
        query_erlang_version=QUERY_ERL_VERSION,
        erlang_version=erlang_version,
        erlang_home=ctx.attr._erlang_home[ErlangHomeProvider].path,
    )

    inputs = []
    inputs.extend(hdrs)
    inputs.extend(srcs)
    for dep in ctx.attr.deps:
        lib_info = dep[ErlangLibInfo]
        inputs.extend(lib_info.include)
        inputs.extend(lib_info.beam)

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = beam_files,
        command = script,
        arguments = [erl_args],
        env = {
            # "ERLANG_VERSION": ctx.attr.erlang_version,
        },
        mnemonic = "ERLC",
    )

    return ErlangLibInfo(
        lib_name = ctx.attr.app_name,
        lib_version = ctx.attr.app_version,
        erlang_version = ctx.attr._erlang_version[ErlangVersionProvider].version,
        include = ctx.files.hdrs, # <- should be filtered public only
        beam = beam_files,
        priv = ctx.files.priv,
    )

def _impl(ctx):
    app_file = _gen_app_file(ctx, ctx.files.srcs)

    erlang_lib_info = compile_erlang_action(ctx, srcs=ctx.files.srcs, hdrs=ctx.files.hdrs)
    erlang_lib_info = ErlangLibInfo(
        lib_name = erlang_lib_info.lib_name,
        lib_version = erlang_lib_info.lib_version,
        erlang_version = erlang_lib_info.erlang_version,
        include = erlang_lib_info.include,
        beam = [app_file] + erlang_lib_info.beam,
        priv = erlang_lib_info.priv,
    )

    return [
        DefaultInfo(files = depset(erlang_lib_info.beam)),
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
        "_erlang_home": attr.label(default = ":erlang_home"),
        "_erlang_version": attr.label(default = ":erlang_version"),
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
