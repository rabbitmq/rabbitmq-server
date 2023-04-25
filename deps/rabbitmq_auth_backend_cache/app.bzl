load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":behaviours", ":other_beam"],
    )
    erlang_bytecode(
        name = "behaviours",
        srcs = ["src/rabbit_auth_cache.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_auth_backend_cache",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/rabbit_auth_backend_cache.erl",
            "src/rabbit_auth_backend_cache_app.erl",
            "src/rabbit_auth_cache_dict.erl",
            "src/rabbit_auth_cache_ets.erl",
            "src/rabbit_auth_cache_ets_segmented.erl",
            "src/rabbit_auth_cache_ets_segmented_stateless.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_auth_backend_cache",
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
        srcs = ["src/rabbit_auth_cache.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_auth_backend_cache",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = [
            "src/rabbit_auth_backend_cache.erl",
            "src/rabbit_auth_backend_cache_app.erl",
            "src/rabbit_auth_cache_dict.erl",
            "src/rabbit_auth_cache_ets.erl",
            "src/rabbit_auth_cache_ets_segmented.erl",
            "src/rabbit_auth_cache_ets_segmented_stateless.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_auth_backend_cache",
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
        srcs = ["priv/schema/rabbitmq_auth_backend_cache.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_auth_backend_cache.erl",
            "src/rabbit_auth_backend_cache_app.erl",
            "src/rabbit_auth_cache.erl",
            "src/rabbit_auth_cache_dict.erl",
            "src/rabbit_auth_cache_ets.erl",
            "src/rabbit_auth_cache_ets_segmented.erl",
            "src/rabbit_auth_cache_ets_segmented_stateless.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbit_auth_backend_cache.hrl"],
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
        app_name = "rabbitmq_auth_backend_cache",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_auth_backend_cache_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_auth_backend_cache_SUITE.erl"],
        outs = ["test/rabbit_auth_backend_cache_SUITE.beam"],
        app_name = "rabbitmq_auth_backend_cache",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_auth_cache_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_auth_cache_SUITE.erl"],
        outs = ["test/rabbit_auth_cache_SUITE.beam"],
        app_name = "rabbitmq_auth_backend_cache",
        erlc_opts = "//:test_erlc_opts",
    )
