load("//bazel_erlang:bazel_erlang_lib.bzl", "ErlangLibInfo", "path_join")
load("//bazel_erlang:ct.bzl", "lib_dir")

RabbitmqHomeInfo = provider(
    doc = "An assembled RABBITMQ_HOME dir",
    fields = {
        'sbin': 'Files making up the sbin dir',
        'escript': 'Files making up the escript dir',
        'plugins': 'Files making up the plugins dir',
    },
)

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
            "plugins",
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

    plugins = [_plugins_dir_link(ctx, plugin) for plugin in ctx.attr._base_plugins + ctx.attr.plugins]

    return [
        RabbitmqHomeInfo(
            sbin = scripts,
            escript = escripts,
            plugins = plugins,
        ),
        DefaultInfo(
            files = depset(scripts + escripts + plugins),
        ),
    ]

rabbitmq_home = rule(
    implementation = _impl,
    attrs = {
        "_scripts": attr.label_list(
            default = [
                "//deps/rabbit:scripts/rabbitmq-env",
                "//deps/rabbit:scripts/rabbitmq-defaults",
                "//deps/rabbit:scripts/rabbitmq-server",
                "//deps/rabbit:scripts/rabbitmqctl",
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
        "plugins": attr.label_list(),
    },
)