load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":behaviours", ":other_beam"],
    )
    erlang_bytecode(
        name = "behaviours",
        srcs = ["src/rabbit_mqtt_retained_msg_store.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_mqtt",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.DecommissionMqttNodeCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListMqttConnectionsCommand.erl",
            "src/mc_mqtt.erl",
            "src/mqtt_machine.erl",
            "src/mqtt_machine_v0.erl",
            "src/mqtt_node.erl",
            "src/rabbit_mqtt.erl",
            "src/rabbit_mqtt_collector.erl",
            "src/rabbit_mqtt_confirms.erl",
            "src/rabbit_mqtt_ff.erl",
            "src/rabbit_mqtt_internal_event_handler.erl",
            "src/rabbit_mqtt_keepalive.erl",
            "src/rabbit_mqtt_packet.erl",
            "src/rabbit_mqtt_processor.erl",
            "src/rabbit_mqtt_qos0_queue.erl",
            "src/rabbit_mqtt_reader.erl",
            "src/rabbit_mqtt_retained_msg_store_dets.erl",
            "src/rabbit_mqtt_retained_msg_store_ets.erl",
            "src/rabbit_mqtt_retained_msg_store_noop.erl",
            "src/rabbit_mqtt_retainer.erl",
            "src/rabbit_mqtt_retainer_sup.erl",
            "src/rabbit_mqtt_sup.erl",
            "src/rabbit_mqtt_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_mqtt",
        beam = [":behaviours"],
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app", "//deps/rabbit:erlang_app", "//deps/rabbit_common:erlang_app", "//deps/rabbitmq_cli:erlang_app", "@ra//:erlang_app", "@ranch//:erlang_app"],
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
        srcs = ["src/rabbit_mqtt_retained_msg_store.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_mqtt",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.DecommissionMqttNodeCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListMqttConnectionsCommand.erl",
            "src/mc_mqtt.erl",
            "src/mqtt_machine.erl",
            "src/mqtt_machine_v0.erl",
            "src/mqtt_node.erl",
            "src/rabbit_mqtt.erl",
            "src/rabbit_mqtt_collector.erl",
            "src/rabbit_mqtt_confirms.erl",
            "src/rabbit_mqtt_ff.erl",
            "src/rabbit_mqtt_internal_event_handler.erl",
            "src/rabbit_mqtt_keepalive.erl",
            "src/rabbit_mqtt_packet.erl",
            "src/rabbit_mqtt_processor.erl",
            "src/rabbit_mqtt_qos0_queue.erl",
            "src/rabbit_mqtt_reader.erl",
            "src/rabbit_mqtt_retained_msg_store_dets.erl",
            "src/rabbit_mqtt_retained_msg_store_ets.erl",
            "src/rabbit_mqtt_retained_msg_store_noop.erl",
            "src/rabbit_mqtt_retainer.erl",
            "src/rabbit_mqtt_retainer_sup.erl",
            "src/rabbit_mqtt_sup.erl",
            "src/rabbit_mqtt_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_mqtt",
        beam = [":test_behaviours"],
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp10_common:erlang_app",
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
            "//deps/rabbitmq_cli:erlang_app",
            "@ra//:erlang_app",
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
        srcs = ["priv/schema/rabbitmq_mqtt.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.DecommissionMqttNodeCommand.erl",
            "src/Elixir.RabbitMQ.CLI.Ctl.Commands.ListMqttConnectionsCommand.erl",
            "src/mc_mqtt.erl",
            "src/mqtt_machine.erl",
            "src/mqtt_machine_v0.erl",
            "src/mqtt_node.erl",
            "src/rabbit_mqtt.erl",
            "src/rabbit_mqtt_collector.erl",
            "src/rabbit_mqtt_confirms.erl",
            "src/rabbit_mqtt_ff.erl",
            "src/rabbit_mqtt_internal_event_handler.erl",
            "src/rabbit_mqtt_keepalive.erl",
            "src/rabbit_mqtt_packet.erl",
            "src/rabbit_mqtt_processor.erl",
            "src/rabbit_mqtt_qos0_queue.erl",
            "src/rabbit_mqtt_reader.erl",
            "src/rabbit_mqtt_retained_msg_store.erl",
            "src/rabbit_mqtt_retained_msg_store_dets.erl",
            "src/rabbit_mqtt_retained_msg_store_ets.erl",
            "src/rabbit_mqtt_retained_msg_store_noop.erl",
            "src/rabbit_mqtt_retainer.erl",
            "src/rabbit_mqtt_retainer_sup.erl",
            "src/rabbit_mqtt_sup.erl",
            "src/rabbit_mqtt_util.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [
            "include/mqtt_machine.hrl",
            "include/mqtt_machine_v0.hrl",
            "include/rabbit_mqtt.hrl",
            "include/rabbit_mqtt_packet.hrl",
        ],
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
        name = "auth_SUITE_beam_files",
        testonly = True,
        srcs = ["test/auth_SUITE.erl"],
        outs = ["test/auth_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "cluster_SUITE_beam_files",
        testonly = True,
        srcs = ["test/cluster_SUITE.erl"],
        outs = ["test/cluster_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "command_SUITE_beam_files",
        testonly = True,
        srcs = ["test/command_SUITE.erl"],
        outs = ["test/command_SUITE.beam"],
        hdrs = ["include/rabbit_mqtt.hrl"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "config_schema_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_schema_SUITE.erl"],
        outs = ["test/config_schema_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "java_SUITE_beam_files",
        testonly = True,
        srcs = ["test/java_SUITE.erl"],
        outs = ["test/java_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "mqtt_machine_SUITE_beam_files",
        testonly = True,
        srcs = ["test/mqtt_machine_SUITE.erl"],
        outs = ["test/mqtt_machine_SUITE.beam"],
        hdrs = ["include/mqtt_machine.hrl"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "processor_SUITE_beam_files",
        testonly = True,
        srcs = ["test/processor_SUITE.erl"],
        outs = ["test/processor_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "proxy_protocol_SUITE_beam_files",
        testonly = True,
        srcs = ["test/proxy_protocol_SUITE.erl"],
        outs = ["test/proxy_protocol_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "reader_SUITE_beam_files",
        testonly = True,
        srcs = ["test/reader_SUITE.erl"],
        outs = ["test/reader_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "retainer_SUITE_beam_files",
        testonly = True,
        srcs = ["test/retainer_SUITE.erl"],
        outs = ["test/retainer_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbit_auth_backend_mqtt_mock_beam",
        testonly = True,
        srcs = ["test/rabbit_auth_backend_mqtt_mock.erl"],
        outs = ["test/rabbit_auth_backend_mqtt_mock.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "util_SUITE_beam_files",
        testonly = True,
        srcs = ["test/util_SUITE.erl"],
        outs = ["test/util_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "config_SUITE_beam_files",
        testonly = True,
        srcs = ["test/config_SUITE.erl"],
        outs = ["test/config_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ff_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ff_SUITE.erl"],
        outs = ["test/ff_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "shared_SUITE_beam_files",
        testonly = True,
        srcs = ["test/shared_SUITE.erl"],
        outs = ["test/shared_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_ct_helpers:erlang_app"],
    )
    erlang_bytecode(
        name = "test_event_recorder_beam",
        testonly = True,
        srcs = ["test/event_recorder.erl"],
        outs = ["test/event_recorder.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "test_util_beam",
        testonly = True,
        srcs = ["test/util.erl"],
        outs = ["test/util.beam"],
        hdrs = ["include/rabbit_mqtt.hrl"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "packet_prop_SUITE_beam_files",
        testonly = True,
        srcs = ["test/packet_prop_SUITE.erl"],
        outs = ["test/packet_prop_SUITE.beam"],
        hdrs = ["include/rabbit_mqtt_packet.hrl"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "v5_SUITE_beam_files",
        testonly = True,
        srcs = ["test/v5_SUITE.erl"],
        outs = ["test/v5_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_mqtt_confirms_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_mqtt_confirms_SUITE.erl"],
        outs = ["test/rabbit_mqtt_confirms_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "mc_mqtt_SUITE_beam_files",
        testonly = True,
        srcs = ["test/mc_mqtt_SUITE.erl"],
        outs = ["test/mc_mqtt_SUITE.beam"],
        hdrs = ["include/rabbit_mqtt_packet.hrl"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp10_common:erlang_app"],
    )
    erlang_bytecode(
        name = "protocol_interop_SUITE_beam_files",
        testonly = True,
        srcs = ["test/protocol_interop_SUITE.erl"],
        outs = ["test/protocol_interop_SUITE.beam"],
        app_name = "rabbitmq_mqtt",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbitmq_stomp:erlang_app"],
    )
