load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode", "erlc_opts")
load("@rules_erlang//:erlang_app.bzl", "erlang_app")

erlc_opts(
    name = "erlc_opts",
    values = select({
        "@rules_erlang//:debug_build": [
            "+debug_info",
        ],
        "//conditions:default": [
            "+debug_info",
            "+deterministic",
        ],
    }),
    visibility = [":__subpackages__"],
)

erlang_bytecode(
    name = "ebin_jsx_beam",
    srcs = ["src/jsx.erl"],
    outs = ["ebin/jsx.beam"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_config_beam",
    srcs = ["src/jsx_config.erl"],
    outs = ["ebin/jsx_config.beam"],
    hdrs = ["src/jsx_config.hrl"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_consult_beam",
    srcs = ["src/jsx_consult.erl"],
    outs = ["ebin/jsx_consult.beam"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_decoder_beam",
    srcs = ["src/jsx_decoder.erl"],
    outs = ["ebin/jsx_decoder.beam"],
    hdrs = ["src/jsx_config.hrl"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_encoder_beam",
    srcs = ["src/jsx_encoder.erl"],
    outs = ["ebin/jsx_encoder.beam"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_parser_beam",
    srcs = ["src/jsx_parser.erl"],
    outs = ["ebin/jsx_parser.beam"],
    hdrs = ["src/jsx_config.hrl"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_to_json_beam",
    srcs = ["src/jsx_to_json.erl"],
    outs = ["ebin/jsx_to_json.beam"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_to_term_beam",
    srcs = ["src/jsx_to_term.erl"],
    outs = ["ebin/jsx_to_term.beam"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

erlang_bytecode(
    name = "ebin_jsx_verify_beam",
    srcs = ["src/jsx_verify.erl"],
    outs = ["ebin/jsx_verify.beam"],
    app_name = "jsx",
    erlc_opts = "//:erlc_opts",
)

filegroup(
    name = "beam_files",
    srcs = [
        "ebin/jsx.beam",
        "ebin/jsx_config.beam",
        "ebin/jsx_consult.beam",
        "ebin/jsx_decoder.beam",
        "ebin/jsx_encoder.beam",
        "ebin/jsx_parser.beam",
        "ebin/jsx_to_json.beam",
        "ebin/jsx_to_term.beam",
        "ebin/jsx_verify.beam",
    ],
)

filegroup(
    name = "srcs",
    srcs = [
        "src/jsx.app.src",
        "src/jsx.erl",
        "src/jsx_config.erl",
        "src/jsx_consult.erl",
        "src/jsx_decoder.erl",
        "src/jsx_encoder.erl",
        "src/jsx_parser.erl",
        "src/jsx_to_json.erl",
        "src/jsx_to_term.erl",
        "src/jsx_verify.erl",
    ],
)

filegroup(
    name = "private_hdrs",
    srcs = ["src/jsx_config.hrl"],
)

filegroup(
    name = "public_hdrs",
    srcs = [],
)

filegroup(
    name = "priv",
    srcs = [],
)

filegroup(
    name = "licenses",
    srcs = ["LICENSE"],
)

filegroup(
    name = "public_and_private_hdrs",
    srcs = [
        ":private_hdrs",
        ":public_hdrs",
    ],
)

filegroup(
    name = "all_srcs",
    srcs = [
        ":public_and_private_hdrs",
        ":srcs",
    ],
)

erlang_app(
    name = "erlang_app",
    srcs = [":all_srcs"],
    app_name = "jsx",
    beam_files = [":beam_files"],
)

alias(
    name = "jsx",
    actual = ":erlang_app",
    visibility = ["//visibility:public"],
)
