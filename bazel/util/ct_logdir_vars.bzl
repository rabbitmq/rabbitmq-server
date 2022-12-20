load(
    "@bazel_skylib//rules:common_settings.bzl",
    "BuildSettingInfo",
)

def _impl(ctx):
    vars = {
        "CT_LOGDIR": ctx.attr._ct_logdir[BuildSettingInfo].value,
    }

    return [platform_common.TemplateVariableInfo(vars)]

ct_logdir_vars = rule(
    implementation = _impl,
    attrs = {
        "_ct_logdir": attr.label(
            default = Label("@rules_erlang//:ct_logdir"),
        ),
    },
    provides = [
        platform_common.TemplateVariableInfo,
    ],
)
