load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")
load("//bazel_erlang:ct.bzl", "lib_dir")

_PLUGINS_DIR = "plugins"

def _plugins_dir_link(ctx, plugin):
    lib_info = plugin[ErlangLibInfo]
    output = ctx.actions.declare_file(path_join(_PLUGINS_DIR, "{}-{}".format(lib_info.lib_name, lib_info.lib_version)))
    ctx.actions.symlink(
        output = output,
        target_file = lib_info.lib_dir,
    )
    return output

def _impl(ctx):
    plugins_dir_contents = [_plugins_dir_link(ctx, plugin) for plugin in ctx.attr._base_plugins + ctx.attr.plugins]

    ctx.actions.expand_template(
        template = ctx.file._run_broker_template,
        output = ctx.outputs.executable,
        substitutions = {
            "{RABBITMQ_SERVER_PATH}": ctx.attr._rabbitmq_server.files.to_list()[0].short_path,
            "{PLUGINS_DIR}": _PLUGINS_DIR,
        },
        is_executable = True,
    )

    runfiles = ctx.attr._rabbitmq_server[DefaultInfo].default_runfiles
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
        "_rabbitmq_server": attr.label(
            default = Label("//:rabbitmq-server"),
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