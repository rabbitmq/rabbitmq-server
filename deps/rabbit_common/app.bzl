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
            "src/gen_server2.erl",
            "src/rabbit_authn_backend.erl",
            "src/rabbit_authz_backend.erl",
            "src/rabbit_password_hashing.erl",
            "src/rabbit_registry_class.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbit_common",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/rabbit_framing_amqp_0_8.erl",
            "src/rabbit_framing_amqp_0_9_1.erl",
        ] + native.glob(
            ["src/**/*.erl"],
            exclude = [
                "src/gen_server2.erl",
                "src/rabbit_authn_backend.erl",
                "src/rabbit_authz_backend.erl",
                "src/rabbit_framing_amqp_0_8.erl",  # keep
                "src/rabbit_framing_amqp_0_9_1.erl",  # keep
                "src/rabbit_password_hashing.erl",
                "src/rabbit_registry_class.erl",
            ],
        ),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbit_common",
        beam = [":behaviours"],
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
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
            "src/gen_server2.erl",
            "src/rabbit_authn_backend.erl",
            "src/rabbit_authz_backend.erl",
            "src/rabbit_password_hashing.erl",
            "src/rabbit_registry_class.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbit_common",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = [
            "src/rabbit_framing_amqp_0_8.erl",
            "src/rabbit_framing_amqp_0_9_1.erl",
        ] + native.glob(
            ["src/**/*.erl"],
            exclude = [
                "src/gen_server2.erl",
                "src/rabbit_authn_backend.erl",
                "src/rabbit_authz_backend.erl",
                "src/rabbit_framing_amqp_0_8.erl",  # keep
                "src/rabbit_framing_amqp_0_9_1.erl",  # keep
                "src/rabbit_password_hashing.erl",
                "src/rabbit_registry_class.erl",
            ],
        ),
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbit_common",
        beam = [":test_behaviours"],
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
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
        name = "srcs",
        srcs = [
            "src/rabbit_framing_amqp_0_8.erl",
            "src/rabbit_framing_amqp_0_9_1.erl",
        ] + native.glob([
            "src/**/*.app.src",
            "src/**/*.erl",
        ], exclude = [
            "src/rabbit_framing_amqp_0_8.erl",  # keep
            "src/rabbit_framing_amqp_0_9_1.erl",  # keep
        ]),
    )
    filegroup(
        name = "public_hdrs",
        srcs = [
            "include/rabbit_framing.hrl",
        ] + native.glob(
            ["include/**/*.hrl"],
            exclude = [
                "include/rabbit_framing.hrl",  # keep
            ],
        ),
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
        name = "rabbit_env_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_env_SUITE.erl"],
        outs = ["test/rabbit_env_SUITE.beam"],
        app_name = "rabbit_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "supervisor2_SUITE_beam_files",
        testonly = True,
        srcs = ["test/supervisor2_SUITE.erl"],
        outs = ["test/supervisor2_SUITE.beam"],
        hdrs = ["include/rabbit.hrl", "include/resource.hrl"],
        app_name = "rabbit_common",
        beam = ["ebin/supervisor2.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_gen_server2_test_server_beam",
        testonly = True,
        srcs = ["test/gen_server2_test_server.erl"],
        outs = ["test/gen_server2_test_server.beam"],
        app_name = "rabbit_common",
        beam = ["ebin/gen_server2.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_test_event_handler_beam",
        testonly = True,
        srcs = ["test/test_event_handler.erl"],
        outs = ["test/test_event_handler.beam"],
        hdrs = ["include/rabbit.hrl", "include/resource.hrl"],
        app_name = "rabbit_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_SUITE.erl"],
        outs = ["test/unit_SUITE.beam"],
        hdrs = ["include/rabbit.hrl", "include/rabbit_memory.hrl", "include/resource.hrl"],
        app_name = "rabbit_common",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "unit_priority_queue_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_priority_queue_SUITE.erl"],
        outs = ["test/unit_priority_queue_SUITE.beam"],
        app_name = "rabbit_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "worker_pool_SUITE_beam_files",
        testonly = True,
        srcs = ["test/worker_pool_SUITE.erl"],
        outs = ["test/worker_pool_SUITE.beam"],
        app_name = "rabbit_common",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "unit_password_hashing_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_password_hashing_SUITE.erl"],
        outs = ["test/unit_password_hashing_SUITE.beam"],
        app_name = "rabbit_common",
        erlc_opts = "//:test_erlc_opts",
    )
