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
            "src/rabbit_tracing_app.erl",
            "src/rabbit_tracing_consumer.erl",
            "src/rabbit_tracing_consumer_sup.erl",
            "src/rabbit_tracing_files.erl",
            "src/rabbit_tracing_mgmt.erl",
            "src/rabbit_tracing_sup.erl",
            "src/rabbit_tracing_traces.erl",
            "src/rabbit_tracing_util.erl",
            "src/rabbit_tracing_wm_file.erl",
            "src/rabbit_tracing_wm_files.erl",
            "src/rabbit_tracing_wm_trace.erl",
            "src/rabbit_tracing_wm_traces.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_tracing",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_management:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
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
            "src/rabbit_tracing_app.erl",
            "src/rabbit_tracing_consumer.erl",
            "src/rabbit_tracing_consumer_sup.erl",
            "src/rabbit_tracing_files.erl",
            "src/rabbit_tracing_mgmt.erl",
            "src/rabbit_tracing_sup.erl",
            "src/rabbit_tracing_traces.erl",
            "src/rabbit_tracing_util.erl",
            "src/rabbit_tracing_wm_file.erl",
            "src/rabbit_tracing_wm_files.erl",
            "src/rabbit_tracing_wm_trace.erl",
            "src/rabbit_tracing_wm_traces.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_tracing",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_management:erlang_app",
            "//deps/rabbitmq_management_agent:erlang_app",
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
            "priv/www/js/tmpl/traces.ejs",
            "priv/www/js/tracing.js",
        ],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_tracing_app.erl",
            "src/rabbit_tracing_consumer.erl",
            "src/rabbit_tracing_consumer_sup.erl",
            "src/rabbit_tracing_files.erl",
            "src/rabbit_tracing_mgmt.erl",
            "src/rabbit_tracing_sup.erl",
            "src/rabbit_tracing_traces.erl",
            "src/rabbit_tracing_util.erl",
            "src/rabbit_tracing_wm_file.erl",
            "src/rabbit_tracing_wm_files.erl",
            "src/rabbit_tracing_wm_trace.erl",
            "src/rabbit_tracing_wm_traces.erl",
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
        name = "rabbit_tracing_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_tracing_SUITE.erl"],
        outs = ["test/rabbit_tracing_SUITE.beam"],
        app_name = "rabbitmq_tracing",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
