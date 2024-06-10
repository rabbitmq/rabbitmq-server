load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":behaviours", ":other_beam"],
    )
    erlang_bytecode(
        name = "behaviours",
        srcs = [
            "src/amqp_gen_connection.erl",
            "src/amqp_gen_consumer.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp_client",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/amqp_auth_mechanisms.erl",
            "src/amqp_channel.erl",
            "src/amqp_channel_sup.erl",
            "src/amqp_channel_sup_sup.erl",
            "src/amqp_channels_manager.erl",
            "src/amqp_client.erl",
            "src/amqp_connection.erl",
            "src/amqp_connection_sup.erl",
            "src/amqp_connection_type_sup.erl",
            "src/amqp_direct_connection.erl",
            "src/amqp_direct_consumer.erl",
            "src/amqp_main_reader.erl",
            "src/amqp_network_connection.erl",
            "src/amqp_rpc_client.erl",
            "src/amqp_rpc_server.erl",
            "src/amqp_selective_consumer.erl",
            "src/amqp_ssl.erl",
            "src/amqp_sup.erl",
            "src/amqp_uri.erl",
            "src/amqp_util.erl",
            "src/rabbit_routing_util.erl",
            "src/uri_parser.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp_client",
        beam = [":behaviours"],
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )

def all_test_beam_files(name = "all_test_beam_files"):
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = [":test_behaviours", ":test_other_beam"],
    )
    erlang_bytecode(
        name = "test_behaviours",
        testonly = True,
        srcs = [
            "src/amqp_gen_connection.erl",
            "src/amqp_gen_consumer.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp_client",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = [
            "src/amqp_auth_mechanisms.erl",
            "src/amqp_channel.erl",
            "src/amqp_channel_sup.erl",
            "src/amqp_channel_sup_sup.erl",
            "src/amqp_channels_manager.erl",
            "src/amqp_client.erl",
            "src/amqp_connection.erl",
            "src/amqp_connection_sup.erl",
            "src/amqp_connection_type_sup.erl",
            "src/amqp_direct_connection.erl",
            "src/amqp_direct_consumer.erl",
            "src/amqp_main_reader.erl",
            "src/amqp_network_connection.erl",
            "src/amqp_rpc_client.erl",
            "src/amqp_rpc_server.erl",
            "src/amqp_selective_consumer.erl",
            "src/amqp_ssl.erl",
            "src/amqp_sup.erl",
            "src/amqp_uri.erl",
            "src/amqp_util.erl",
            "src/rabbit_routing_util.erl",
            "src/uri_parser.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp_client",
        beam = [":test_behaviours"],
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
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
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/amqp_auth_mechanisms.erl",
            "src/amqp_channel.erl",
            "src/amqp_channel_sup.erl",
            "src/amqp_channel_sup_sup.erl",
            "src/amqp_channels_manager.erl",
            "src/amqp_client.erl",
            "src/amqp_connection.erl",
            "src/amqp_connection_sup.erl",
            "src/amqp_connection_type_sup.erl",
            "src/amqp_direct_connection.erl",
            "src/amqp_direct_consumer.erl",
            "src/amqp_gen_connection.erl",
            "src/amqp_gen_consumer.erl",
            "src/amqp_main_reader.erl",
            "src/amqp_network_connection.erl",
            "src/amqp_rpc_client.erl",
            "src/amqp_rpc_server.erl",
            "src/amqp_selective_consumer.erl",
            "src/amqp_ssl.erl",
            "src/amqp_sup.erl",
            "src/amqp_uri.erl",
            "src/amqp_util.erl",
            "src/rabbit_routing_util.erl",
            "src/uri_parser.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [
            "include/amqp_client.hrl",
            "include/amqp_client_internal.hrl",
            "include/amqp_gen_consumer_spec.hrl",
            "include/rabbit_routing_prefixes.hrl",
        ],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "license_files",
        srcs = [
            "LICENSE",
            "LICENSE-MPL-RabbitMQ",
        ],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        hdrs = ["include/amqp_client.hrl", "include/amqp_client_internal.hrl"],
        app_name = "amqp_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_SUITE.erl"],
        outs = ["test/unit_SUITE.beam"],
        hdrs = ["include/amqp_client.hrl"],
        app_name = "amqp_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
