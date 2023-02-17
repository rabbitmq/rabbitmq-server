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
    name = "other_beam",
    srcs = [
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
    outs = [
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
    hdrs = ["src/jsx_config.hrl"],
    app_name = "jsx",
    beam = [],
    erlc_opts = "//:erlc_opts",
)

filegroup(
    name = "beam_files",
    srcs = [":other_beam"],
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
