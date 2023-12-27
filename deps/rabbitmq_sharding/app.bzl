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
            "src/rabbit_sharding_exchange_decorator.erl",
            "src/rabbit_sharding_exchange_type_modulus_hash.erl",
            "src/rabbit_sharding_interceptor.erl",
            "src/rabbit_sharding_policy_validator.erl",
            "src/rabbit_sharding_shard.erl",
            "src/rabbit_sharding_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_sharding",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
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
            "src/rabbit_sharding_exchange_decorator.erl",
            "src/rabbit_sharding_exchange_type_modulus_hash.erl",
            "src/rabbit_sharding_interceptor.erl",
            "src/rabbit_sharding_policy_validator.erl",
            "src/rabbit_sharding_shard.erl",
            "src/rabbit_sharding_util.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_sharding",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = [
            "//deps/rabbit:erlang_app",
            "//deps/rabbit_common:erlang_app",
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
            "src/rabbit_sharding_exchange_decorator.erl",
            "src/rabbit_sharding_exchange_type_modulus_hash.erl",
            "src/rabbit_sharding_interceptor.erl",
            "src/rabbit_sharding_policy_validator.erl",
            "src/rabbit_sharding_shard.erl",
            "src/rabbit_sharding_util.erl",
        ],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "public_hdrs",
    )
    filegroup(
        name = "license_files",
        srcs = [
            "LICENSE",
            "LICENSE-MPL-RabbitMQ",
            "LICENSE-MPL2",
        ],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "rabbit_hash_exchange_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_hash_exchange_SUITE.erl"],
        outs = ["test/rabbit_hash_exchange_SUITE.beam"],
        app_name = "rabbitmq_sharding",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
    erlang_bytecode(
        name = "rabbit_sharding_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_sharding_SUITE.erl"],
        outs = ["test/rabbit_sharding_SUITE.beam"],
        app_name = "rabbitmq_sharding",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app", "//deps/rabbit:erlang_app"],
    )
