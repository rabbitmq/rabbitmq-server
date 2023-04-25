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
            "src/rabbit_top_app.erl",
            "src/rabbit_top_extension.erl",
            "src/rabbit_top_sup.erl",
            "src/rabbit_top_util.erl",
            "src/rabbit_top_wm_ets_tables.erl",
            "src/rabbit_top_wm_process.erl",
            "src/rabbit_top_wm_processes.erl",
            "src/rabbit_top_worker.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_top",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
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
            "priv/www/js/tmpl/ets_tables.ejs",
            "priv/www/js/tmpl/process.ejs",
            "priv/www/js/tmpl/processes.ejs",
            "priv/www/js/top.js",
        ],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_top_app.erl",
            "src/rabbit_top_extension.erl",
            "src/rabbit_top_sup.erl",
            "src/rabbit_top_util.erl",
            "src/rabbit_top_wm_ets_tables.erl",
            "src/rabbit_top_wm_process.erl",
            "src/rabbit_top_wm_processes.erl",
            "src/rabbit_top_worker.erl",
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
            "src/rabbit_top_app.erl",
            "src/rabbit_top_extension.erl",
            "src/rabbit_top_sup.erl",
            "src/rabbit_top_util.erl",
            "src/rabbit_top_wm_ets_tables.erl",
            "src/rabbit_top_wm_process.erl",
            "src/rabbit_top_wm_processes.erl",
            "src/rabbit_top_worker.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_top",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbit_common:erlang_app", "//deps/rabbitmq_management:erlang_app", "//deps/rabbitmq_management_agent:erlang_app"],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    pass
