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
            "src/rabbit_cowboy_middleware.erl",
            "src/rabbit_cowboy_redirect.erl",
            "src/rabbit_cowboy_stream_h.erl",
            "src/rabbit_web_dispatch.erl",
            "src/rabbit_web_dispatch_access_control.erl",
            "src/rabbit_web_dispatch_app.erl",
            "src/rabbit_web_dispatch_listing_handler.erl",
            "src/rabbit_web_dispatch_registry.erl",
            "src/rabbit_web_dispatch_sup.erl",
            "src/rabbit_web_dispatch_util.erl",
            "src/webmachine_log.erl",
            "src/webmachine_log_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_dispatch",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "@cowboy//:erlang_app",
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
            "src/rabbit_cowboy_middleware.erl",
            "src/rabbit_cowboy_redirect.erl",
            "src/rabbit_cowboy_stream_h.erl",
            "src/rabbit_web_dispatch.erl",
            "src/rabbit_web_dispatch_access_control.erl",
            "src/rabbit_web_dispatch_app.erl",
            "src/rabbit_web_dispatch_listing_handler.erl",
            "src/rabbit_web_dispatch_registry.erl",
            "src/rabbit_web_dispatch_sup.erl",
            "src/rabbit_web_dispatch_util.erl",
            "src/webmachine_log.erl",
            "src/webmachine_log_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_dispatch",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/amqp_client:erlang_app",
            "@cowboy//:erlang_app",
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
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_cowboy_middleware.erl",
            "src/rabbit_cowboy_redirect.erl",
            "src/rabbit_cowboy_stream_h.erl",
            "src/rabbit_web_dispatch.erl",
            "src/rabbit_web_dispatch_access_control.erl",
            "src/rabbit_web_dispatch_app.erl",
            "src/rabbit_web_dispatch_listing_handler.erl",
            "src/rabbit_web_dispatch_registry.erl",
            "src/rabbit_web_dispatch_sup.erl",
            "src/rabbit_web_dispatch_util.erl",
            "src/webmachine_log.erl",
            "src/webmachine_log_handler.erl",
        ],
    )
    filegroup(
        name = "private_hdrs",
        srcs = ["src/webmachine_logger.hrl"],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbitmq_web_dispatch_records.hrl"],
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
        name = "rabbit_web_dispatch_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_web_dispatch_SUITE.erl"],
        outs = ["test/rabbit_web_dispatch_SUITE.beam"],
        app_name = "rabbitmq_web_dispatch",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_web_dispatch_unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_web_dispatch_unit_SUITE.erl"],
        outs = ["test/rabbit_web_dispatch_unit_SUITE.beam"],
        app_name = "rabbitmq_web_dispatch",
        erlc_opts = "//:test_erlc_opts",
    )
