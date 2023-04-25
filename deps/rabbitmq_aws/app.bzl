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
            "src/rabbitmq_aws.erl",
            "src/rabbitmq_aws_app.erl",
            "src/rabbitmq_aws_config.erl",
            "src/rabbitmq_aws_json.erl",
            "src/rabbitmq_aws_sign.erl",
            "src/rabbitmq_aws_sup.erl",
            "src/rabbitmq_aws_urilib.erl",
            "src/rabbitmq_aws_xml.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_aws",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
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
            "src/rabbitmq_aws.erl",
            "src/rabbitmq_aws_app.erl",
            "src/rabbitmq_aws_config.erl",
            "src/rabbitmq_aws_json.erl",
            "src/rabbitmq_aws_sign.erl",
            "src/rabbitmq_aws_sup.erl",
            "src/rabbitmq_aws_urilib.erl",
            "src/rabbitmq_aws_xml.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_aws",
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
        srcs = ["priv/schema/rabbitmq_aws.schema"],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "srcs",
        srcs = [
            "src/rabbitmq_aws.erl",
            "src/rabbitmq_aws_app.erl",
            "src/rabbitmq_aws_config.erl",
            "src/rabbitmq_aws_json.erl",
            "src/rabbitmq_aws_sign.erl",
            "src/rabbitmq_aws_sup.erl",
            "src/rabbitmq_aws_urilib.erl",
            "src/rabbitmq_aws_xml.erl",
        ],
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/rabbitmq_aws.hrl"],
    )
    filegroup(
        name = "license_files",
        srcs = [
            "LICENSE",
            "LICENSE-erlcloud",
            "LICENSE-httpc_aws",
            "LICENSE-rabbitmq_aws",
        ],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "test_rabbitmq_aws_all_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_all_tests.erl"],
        outs = ["test/rabbitmq_aws_all_tests.beam"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_app_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_app_tests.erl"],
        outs = ["test/rabbitmq_aws_app_tests.beam"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_config_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_config_tests.erl"],
        outs = ["test/rabbitmq_aws_config_tests.beam"],
        hdrs = ["include/rabbitmq_aws.hrl"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_json_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_json_tests.erl"],
        outs = ["test/rabbitmq_aws_json_tests.beam"],
        hdrs = ["include/rabbitmq_aws.hrl"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_sign_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_sign_tests.erl"],
        outs = ["test/rabbitmq_aws_sign_tests.beam"],
        hdrs = ["include/rabbitmq_aws.hrl"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_sup_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_sup_tests.erl"],
        outs = ["test/rabbitmq_aws_sup_tests.beam"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_tests.erl"],
        outs = ["test/rabbitmq_aws_tests.beam"],
        hdrs = ["include/rabbitmq_aws.hrl"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_urilib_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_urilib_tests.erl"],
        outs = ["test/rabbitmq_aws_urilib_tests.beam"],
        hdrs = ["include/rabbitmq_aws.hrl"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_rabbitmq_aws_xml_tests_beam",
        testonly = True,
        srcs = ["test/rabbitmq_aws_xml_tests.erl"],
        outs = ["test/rabbitmq_aws_xml_tests.beam"],
        hdrs = ["include/rabbitmq_aws.hrl"],
        app_name = "rabbitmq_aws",
        erlc_opts = "//:test_erlc_opts",
    )
