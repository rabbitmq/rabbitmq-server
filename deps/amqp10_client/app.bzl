load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/amqp10_client.erl",
            "src/amqp10_client_app.erl",
            "src/amqp10_client_connection.erl",
            "src/amqp10_client_connection_sup.erl",
            "src/amqp10_client_connections_sup.erl",
            "src/amqp10_client_frame_reader.erl",
            "src/amqp10_client_session.erl",
            "src/amqp10_client_sessions_sup.erl",
            "src/amqp10_client_sup.erl",
            "src/amqp10_client_types.erl",
            "src/amqp10_msg.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp10_client",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
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
        srcs = [
            "src/amqp10_client.erl",
            "src/amqp10_client_app.erl",
            "src/amqp10_client_connection.erl",
            "src/amqp10_client_connection_sup.erl",
            "src/amqp10_client_connections_sup.erl",
            "src/amqp10_client_frame_reader.erl",
            "src/amqp10_client_session.erl",
            "src/amqp10_client_sessions_sup.erl",
            "src/amqp10_client_sup.erl",
            "src/amqp10_client_types.erl",
            "src/amqp10_msg.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "amqp10_client",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
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
            "src/amqp10_client.erl",
            "src/amqp10_client_app.erl",
            "src/amqp10_client_connection.erl",
            "src/amqp10_client_connection_sup.erl",
            "src/amqp10_client_connections_sup.erl",
            "src/amqp10_client_frame_reader.erl",
            "src/amqp10_client_session.erl",
            "src/amqp10_client_sessions_sup.erl",
            "src/amqp10_client_sup.erl",
            "src/amqp10_client_types.erl",
            "src/amqp10_msg.erl",
        ],
    )
    filegroup(
        name = "private_hdrs",
        srcs = ["src/amqp10_client.hrl"],
    )
    filegroup(
        name = "public_hdrs",
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
        name = "msg_SUITE_beam_files",
        testonly = True,
        srcs = ["test/msg_SUITE.erl"],
        outs = ["test/msg_SUITE.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_activemq_ct_helpers_beam",
        testonly = True,
        srcs = ["test/activemq_ct_helpers.erl"],
        outs = ["test/activemq_ct_helpers.beam"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_mock_server_beam",
        testonly = True,
        srcs = ["test/mock_server.erl"],
        outs = ["test/mock_server.beam"],
        hdrs = ["src/amqp10_client.hrl"],
        app_name = "amqp10_client",
        erlc_opts = "//:test_erlc_opts",
    )
