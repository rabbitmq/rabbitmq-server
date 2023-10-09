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
            "src/rabbit_stream_connection_consumers_mgmt.erl",
            "src/rabbit_stream_connection_mgmt.erl",
            "src/rabbit_stream_connection_publishers_mgmt.erl",
            "src/rabbit_stream_connections_mgmt.erl",
            "src/rabbit_stream_connections_vhost_mgmt.erl",
            "src/rabbit_stream_consumers_mgmt.erl",
            "src/rabbit_stream_management_utils.erl",
            "src/rabbit_stream_mgmt_db.erl",
            "src/rabbit_stream_publishers_mgmt.erl",
            "src/rabbit_stream_tracking_mgmt.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stream_management",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_management:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
            "//deps/rabbitmq_stream:erlang_app",
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
        srcs = [
            "src/rabbit_stream_connection_consumers_mgmt.erl",
            "src/rabbit_stream_connection_mgmt.erl",
            "src/rabbit_stream_connection_publishers_mgmt.erl",
            "src/rabbit_stream_connections_mgmt.erl",
            "src/rabbit_stream_connections_vhost_mgmt.erl",
            "src/rabbit_stream_consumers_mgmt.erl",
            "src/rabbit_stream_management_utils.erl",
            "src/rabbit_stream_mgmt_db.erl",
            "src/rabbit_stream_publishers_mgmt.erl",
            "src/rabbit_stream_tracking_mgmt.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_stream_management",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_management:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
            "//deps/rabbitmq_stream:erlang_app",
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
        srcs = [
            "priv/www/js/stream.js",
            "priv/www/js/tmpl/streamConnection.ejs",
            "priv/www/js/tmpl/streamConnections.ejs",
            "priv/www/js/tmpl/streamConsumersList.ejs",
            "priv/www/js/tmpl/streamPublishersList.ejs",
        ],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_stream_connection_consumers_mgmt.erl",
            "src/rabbit_stream_connection_mgmt.erl",
            "src/rabbit_stream_connection_publishers_mgmt.erl",
            "src/rabbit_stream_connections_mgmt.erl",
            "src/rabbit_stream_connections_vhost_mgmt.erl",
            "src/rabbit_stream_consumers_mgmt.erl",
            "src/rabbit_stream_management_utils.erl",
            "src/rabbit_stream_mgmt_db.erl",
            "src/rabbit_stream_publishers_mgmt.erl",
            "src/rabbit_stream_tracking_mgmt.erl",
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
        name = "http_SUITE_beam_files",
        testonly = True,
        srcs = ["test/http_SUITE.erl"],
        outs = ["test/http_SUITE.beam"],
        app_name = "rabbitmq_stream_management",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
