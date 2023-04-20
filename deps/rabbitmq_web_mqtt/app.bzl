load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = native.glob(["src/**/*.erl"]),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_mqtt",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "@cowboy//:erlang_app",
            "@ranch//:erlang_app",
        ],
    )

def all_test_beam_files(name = "all_test_beam_files"):
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = [":test_other_beam"],
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = native.glob(["src/**/*.erl"]),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_mqtt",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "@cowboy//:erlang_app",
            "@ranch//:erlang_app",
        ],
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "all_srcs",
        srcs = [":public_and_private_hdrs", ":srcs"],
    )
    filegroup(
        name = "public_and_private_hdrs",
        srcs = [":private_hdrs", ":public_hdrs"],
    )

    filegroup(
        name = "priv",
        srcs = native.glob(["priv/**/*"]),
    )
    filegroup(
        name = "public_hdrs",
        srcs = native.glob(["include/**/*.hrl"]),
    )

    filegroup(
        name = "srcs",
        srcs = native.glob([
            "src/**/*.app.src",
            "src/**/*.erl",
        ]),
    )
    filegroup(
        name = "private_hdrs",
        srcs = native.glob(["src/**/*.hrl"]),
    )
    filegroup(
        name = "license_files",
        srcs = native.glob(["LICENSE*"]),
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "proxy_protocol_SUITE_beam_files",
        testonly = True,
        srcs = ["test/proxy_protocol_SUITE.erl"],
        outs = ["test/proxy_protocol_SUITE.beam"],
        hdrs = ["test/src/emqttc_packet.hrl"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        hdrs = ["test/src/emqttc_packet.hrl"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )

    erlang_bytecode(
        name = "test_src_rabbit_ws_test_util_beam",
        testonly = True,
        srcs = ["test/src/rabbit_ws_test_util.erl"],
        outs = ["test/src/rabbit_ws_test_util.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_rfc6455_client_beam",
        testonly = True,
        srcs = ["test/src/rfc6455_client.erl"],
        outs = ["test/src/rfc6455_client.beam"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_emqttc_parser_beam",
        testonly = True,
        srcs = ["test/src/emqttc_parser.erl"],
        outs = ["test/src/emqttc_parser.beam"],
        hdrs = ["test/src/emqttc_packet.hrl"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_src_emqttc_serialiser_beam",
        testonly = True,
        srcs = ["test/src/emqttc_serialiser.erl"],
        outs = ["test/src/emqttc_serialiser.beam"],
        hdrs = ["test/src/emqttc_packet.hrl"],
        app_name = "rabbitmq_web_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
