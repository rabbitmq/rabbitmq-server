load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = [":behaviours", ":other_beam"],
    )
    erlang_bytecode(
        name = "behaviours",
        srcs = ["src/rabbit_trust_store_certificate_provider.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_trust_store",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "other_beam",
        srcs = [
            "src/rabbit_trust_store.erl",
            "src/rabbit_trust_store_app.erl",
            "src/rabbit_trust_store_file_provider.erl",
            "src/rabbit_trust_store_http_provider.erl",
            "src/rabbit_trust_store_sup.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_trust_store",
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
        srcs = ["src/rabbit_trust_store_certificate_provider.erl"],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_trust_store",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_other_beam",
        testonly = True,
        srcs = [
            "src/rabbit_trust_store.erl",
            "src/rabbit_trust_store_app.erl",
            "src/rabbit_trust_store_file_provider.erl",
            "src/rabbit_trust_store_http_provider.erl",
            "src/rabbit_trust_store_sup.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_trust_store",
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
        srcs = ["priv/schema/rabbitmq_trust_store.schema"],
    )
    filegroup(
        name = "public_hdrs",
    )

    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbit_trust_store.erl",
            "src/rabbit_trust_store_app.erl",
            "src/rabbit_trust_store_certificate_provider.erl",
            "src/rabbit_trust_store_file_provider.erl",
            "src/rabbit_trust_store_http_provider.erl",
            "src/rabbit_trust_store_sup.erl",
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
        app_name = "rabbitmq_trust_store",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        app_name = "rabbitmq_trust_store",
        erlc_opts = "//:test_erlc_opts",
        deps = ["//deps/amqp_client:erlang_app"],
    )
