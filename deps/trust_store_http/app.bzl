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
            "src/trust_store_http.erl",
            "src/trust_store_http_app.erl",
            "src/trust_store_http_sup.erl",
            "src/trust_store_invalid_handler.erl",
            "src/trust_store_list_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "trust_store_http",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["@cowboy//:erlang_app"],
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
        name = "srcs",
        srcs = [
            "src/trust_store_http.erl",
            "src/trust_store_http_app.erl",
            "src/trust_store_http_sup.erl",
            "src/trust_store_invalid_handler.erl",
            "src/trust_store_list_handler.erl",
        ],
    )
    filegroup(
        name = "priv",
    )
    filegroup(
        name = "private_hdrs",
    )
    filegroup(
        name = "public_hdrs",
    )
    filegroup(
        name = "license_files",
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
            "src/trust_store_http.erl",
            "src/trust_store_http_app.erl",
            "src/trust_store_http_sup.erl",
            "src/trust_store_invalid_handler.erl",
            "src/trust_store_list_handler.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "trust_store_http",
        dest = "test",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@cowboy//:erlang_app"],
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    pass
