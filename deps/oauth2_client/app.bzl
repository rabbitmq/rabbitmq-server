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
            "src/jwt_helper.erl",
            "src/oauth2_client.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "oauth2_client",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["@jose//:erlang_app"],
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
            "src/jwt_helper.erl",
            "src/oauth2_client.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "oauth2_client",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@jose//:erlang_app"],
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
            "src/jwt_helper.erl",
            "src/oauth2_client.erl",
        ],
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "public_hdrs",
        srcs = ["include/oauth2_client.hrl", "include/types.hrl"],
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
        name = "test_oauth_http_mock_beam",
        testonly = True,
        srcs = ["test/oauth_http_mock.erl"],
        outs = ["test/oauth_http_mock.beam"],
        app_name = "oauth2_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/system_SUITE.erl"],
        outs = ["test/system_SUITE.beam"],
        hdrs = ["include/oauth2_client.hrl", "include/types.hrl"],
        app_name = "oauth2_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "unit_SUITE_beam_files",
        testonly = True,
        srcs = ["test/unit_SUITE.erl"],
        outs = ["test/unit_SUITE.beam"],
        hdrs = ["include/oauth2_client.hrl", "include/types.hrl"],
        app_name = "oauth2_client",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_oauth2_client_test_util_beam",
        testonly = True,
        srcs = ["test/oauth2_client_test_util.erl"],
        outs = ["test/oauth2_client_test_util.beam"],
        app_name = "oauth2_client",
        erlc_opts = "//:test_erlc_opts",
    )
