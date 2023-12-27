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
            "src/collectors/prometheus_process_collector.erl",
            "src/collectors/prometheus_rabbitmq_alarm_metrics_collector.erl",
            "src/collectors/prometheus_rabbitmq_core_metrics_collector.erl",
            "src/collectors/prometheus_rabbitmq_dynamic_collector.erl",
            "src/collectors/prometheus_rabbitmq_global_metrics_collector.erl",
            "src/rabbit_prometheus_app.erl",
            "src/rabbit_prometheus_dispatcher.erl",
            "src/rabbit_prometheus_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_prometheus",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_web_dispatch:erlang_app",
            "@prometheus//:erlang_app",
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
            "src/collectors/prometheus_process_collector.erl",
            "src/collectors/prometheus_rabbitmq_alarm_metrics_collector.erl",
            "src/collectors/prometheus_rabbitmq_core_metrics_collector.erl",
            "src/collectors/prometheus_rabbitmq_dynamic_collector.erl",
            "src/collectors/prometheus_rabbitmq_global_metrics_collector.erl",
            "src/rabbit_prometheus_app.erl",
            "src/rabbit_prometheus_dispatcher.erl",
            "src/rabbit_prometheus_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_prometheus",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_web_dispatch:erlang_app",
            "@prometheus//:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_prometheus.schema"],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/collectors/prometheus_process_collector.erl",
            "src/collectors/prometheus_rabbitmq_alarm_metrics_collector.erl",
            "src/collectors/prometheus_rabbitmq_core_metrics_collector.erl",
            "src/collectors/prometheus_rabbitmq_dynamic_collector.erl",
            "src/collectors/prometheus_rabbitmq_global_metrics_collector.erl",
            "src/rabbit_prometheus_app.erl",
            "src/rabbit_prometheus_dispatcher.erl",
            "src/rabbit_prometheus_handler.erl",
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
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_prometheus",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_prometheus_http_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_prometheus_http_SUITE.erl"],
        outs = ["test/rabbit_prometheus_http_SUITE.beam"],
        app_name = "rabbitmq_prometheus",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "//deps/rabbitmq_ct_helpers:erlang_app",
        ],
    )
