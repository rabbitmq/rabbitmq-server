load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":other_beam"],
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = ["src/rabbit_web_stomp_examples_app.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_stomp_examples",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
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
            "priv/bunny.html",
            "priv/bunny.png",
            "priv/echo.html",
            "priv/index.html",
            "priv/main.css",
            "priv/pencil.cur",
            "priv/stomp.js",
            "priv/temp-queue.html",
        ],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = ["src/rabbit_web_stomp_examples_app.erl"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "license_files",
        srcs = [
            "LICENSE",
            "LICENSE-APL2-Stomp-Websocket",
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
        srcs = ["src/rabbit_web_stomp_examples_app.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_web_stomp_examples",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    pass
