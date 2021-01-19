load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")
load("//bazel_erlang:ct.bzl", "lib_dir")

_PLUGINS_DIR = "plugins"

def _copy_script(ctx, script):
    dest = ctx.actions.declare_file(path_join("sbin", script.basename))
    args = ctx.actions.args()
    args.add_all([script, dest])
    ctx.actions.run(
        inputs = [script],
        outputs = [dest],
        executable = "cp",
        arguments = [args],
        mnemonic = "COPY",
        progress_message = "Copying script from deps/rabbit/scripts",
    )
    return dest

def _link_escript(ctx, escript):
    e = escript[DefaultInfo].files.to_list()[0]
    s = ctx.actions.declare_file(path_join("escript", e.basename))
    ctx.actions.symlink(
        output = s,
        target_file = e,
    )
    return s

def _plugins_dir_link(ctx, plugin):
    lib_info = plugin[ErlangLibInfo]
    output = ctx.actions.declare_file(
        path_join(
            _PLUGINS_DIR,
            "{}-{}".format(lib_info.lib_name, lib_info.lib_version),
        )
    )
    ctx.actions.symlink(
        output = output,
        target_file = lib_info.lib_dir,
    )
    return output

def _impl(ctx):
    scripts = [_copy_script(ctx, script) for script in ctx.files._scripts]

    escripts = [_link_escript(ctx, escript) for escript in ctx.attr._escripts]

    plugins_dir_contents = [_plugins_dir_link(ctx, plugin) for plugin in ctx.attr._base_plugins + ctx.attr.plugins]

    ctx.actions.expand_template(
        template = ctx.file._run_broker_template,
        output = ctx.outputs.executable,
        substitutions = {
            "{RABBITMQ_SERVER_PATH}": "sbin/rabbitmq-server",
            "{PLUGINS_DIR}": _PLUGINS_DIR,
        },
        is_executable = True,
    )

    runfiles = ctx.runfiles(scripts + escripts)
    runfiles = runfiles.merge(ctx.runfiles(plugins_dir_contents))

    return [DefaultInfo(
        runfiles = runfiles,
    )]

# this rule (should) creates a test node directory and wrapper around
# rabbitmq-server (in sbin dir) that sets the missing env vars
rabbitmq_node = rule(
    implementation = _impl,
    attrs = {
        "_run_broker_template": attr.label(
            default = Label("//:scripts/bazel/run_broker_impl.sh"),
            allow_single_file = True,
        ),
        "_scripts": attr.label_list(
            default = [
                "//deps/rabbit:scripts/rabbitmq-env",
                "//deps/rabbit:scripts/rabbitmq-defaults",
                "//deps/rabbit:scripts/rabbitmq-server",
            ],
            allow_files = True,
        ),
        "_escripts": attr.label_list(
            default = [
                Label("//deps/rabbitmq_cli:rabbitmqctl"),
            ],
        ),
        # Maybe we should not have to declare the deps here that rabbit/rabbit_common declare
        "_base_plugins": attr.label_list(
            default = [
                Label("@cuttlefish//:cuttlefish"),
                Label("@ranch//:ranch"),
                Label("@lager//:lager"),
                Label("//deps/rabbit_common:rabbit_common"),
                Label("@ra//:ra"),
                Label("@sysmon-handler//:sysmon-handler"),
                Label("@stdout_formatter//:stdout_formatter"),
                Label("@recon//:recon"),
                Label("@observer_cli//:observer_cli"),
                Label("@osiris//:osiris"),
                Label("//deps/amqp10_common:amqp10_common"),
                Label("//deps/rabbit:rabbit"),
                Label("//deps/rabbit/apps/rabbitmq_prelaunch:rabbitmq_prelaunch"),
                Label("@goldrush//:goldrush"),
                Label("@jsx//:jsx"),
                Label("@credentials-obfuscation//:credentials-obfuscation"),
                Label("@aten//:aten"),
                Label("@gen-batch-server//:gen-batch-server"),
            ],
        ),
        "plugins": attr.label_list(
            default = [
                # default to rabbitmq_management
                Label("//deps/rabbitmq_management:rabbitmq_management"),
                Label("//deps/rabbitmq_management_agent:rabbitmq_management_agent"),
                Label("//deps/rabbitmq_web_dispatch:rabbitmq_web_dispatch"),
                Label("//deps/amqp_client:amqp_client"),
                Label("@cowboy//:cowboy"),
                Label("@cowlib//:cowlib"),
            ],
        ),
    },
    executable = True,
)