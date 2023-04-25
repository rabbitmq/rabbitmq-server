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
            "src/rabbit_boot_state.erl",
            "src/rabbit_boot_state_sup.erl",
            "src/rabbit_boot_state_systemd.erl",
            "src/rabbit_boot_state_xterm_titlebar.erl",
            "src/rabbit_logger_fmt_helpers.erl",
            "src/rabbit_logger_json_fmt.erl",
            "src/rabbit_logger_std_h.erl",
            "src/rabbit_logger_text_fmt.erl",
            "src/rabbit_prelaunch.erl",
            "src/rabbit_prelaunch_app.erl",
            "src/rabbit_prelaunch_conf.erl",
            "src/rabbit_prelaunch_dist.erl",
            "src/rabbit_prelaunch_early_logging.erl",
            "src/rabbit_prelaunch_erlang_compat.erl",
            "src/rabbit_prelaunch_errors.erl",
            "src/rabbit_prelaunch_file.erl",
            "src/rabbit_prelaunch_sighandler.erl",
            "src/rabbit_prelaunch_sup.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_prelaunch",
        dest = "ebin",
        erlc_opts = "//:erlc_opts",
        deps = ["//deps/rabbit_common:erlang_app"],
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
            "src/rabbit_boot_state.erl",
            "src/rabbit_boot_state_sup.erl",
            "src/rabbit_boot_state_systemd.erl",
            "src/rabbit_boot_state_xterm_titlebar.erl",
            "src/rabbit_logger_fmt_helpers.erl",
            "src/rabbit_logger_json_fmt.erl",
            "src/rabbit_logger_std_h.erl",
            "src/rabbit_logger_text_fmt.erl",
            "src/rabbit_prelaunch.erl",
            "src/rabbit_prelaunch_app.erl",
            "src/rabbit_prelaunch_conf.erl",
            "src/rabbit_prelaunch_dist.erl",
            "src/rabbit_prelaunch_early_logging.erl",
            "src/rabbit_prelaunch_erlang_compat.erl",
            "src/rabbit_prelaunch_errors.erl",
            "src/rabbit_prelaunch_file.erl",
            "src/rabbit_prelaunch_sighandler.erl",
            "src/rabbit_prelaunch_sup.erl",
        ],
        hdrs = [":public_and_private_hdrs"],
        app_name = "rabbitmq_prelaunch",
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
        name = "srcs",
        srcs = [
            "src/rabbit_boot_state.erl",
            "src/rabbit_boot_state_sup.erl",
            "src/rabbit_boot_state_systemd.erl",
            "src/rabbit_boot_state_xterm_titlebar.erl",
            "src/rabbit_logger_fmt_helpers.erl",
            "src/rabbit_logger_json_fmt.erl",
            "src/rabbit_logger_std_h.erl",
            "src/rabbit_logger_text_fmt.erl",
            "src/rabbit_prelaunch.erl",
            "src/rabbit_prelaunch_app.erl",
            "src/rabbit_prelaunch_conf.erl",
            "src/rabbit_prelaunch_dist.erl",
            "src/rabbit_prelaunch_early_logging.erl",
            "src/rabbit_prelaunch_erlang_compat.erl",
            "src/rabbit_prelaunch_errors.erl",
            "src/rabbit_prelaunch_file.erl",
            "src/rabbit_prelaunch_sighandler.erl",
            "src/rabbit_prelaunch_sup.erl",
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

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "rabbit_logger_std_h_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_logger_std_h_SUITE.erl"],
        outs = ["test/rabbit_logger_std_h_SUITE.beam"],
        app_name = "rabbitmq_prelaunch",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "rabbit_prelaunch_file_SUITE_beam_files",
        testonly = True,
        srcs = ["test/rabbit_prelaunch_file_SUITE.erl"],
        outs = ["test/rabbit_prelaunch_file_SUITE.beam"],
        app_name = "rabbitmq_prelaunch",
        erlc_opts = "//:test_erlc_opts",
    )
