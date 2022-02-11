load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")
load("@rules_erlang//:github.bzl", "github_erlang_app")
load("@rules_erlang//:hex_archive.bzl", "hex_archive")
load("@rules_erlang//:hex_pm.bzl", "hex_pm_erlang_app")
load("//:rabbitmq.bzl", "APP_VERSION")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    hex_pm_erlang_app(
        name = "accept",
        version = "0.3.5",
        sha256 = "11b18c220bcc2eab63b5470c038ef10eb6783bcb1fcdb11aa4137defa5ac1bb8",
    )

    github_erlang_app(
        name = "aten",
        org = "rabbitmq",
        sha256 = "27f6b2ec2e78027ea852a8ac6bcf49df4a599d5506a86dc9f0cb6b5d6e45989e",
        ref = "v0.5.6",
        version = "0.5.6",
    )

    hex_pm_erlang_app(
        name = "base64url",
        version = "1.0.1",
        sha256 = "f9b3add4731a02a9b0410398b475b33e7566a695365237a6bdee1bb447719f5c",
    )

    new_git_repository(
        name = "bats",
        remote = "https://github.com/sstephenson/bats",
        tag = "v0.4.0",
        build_file = rabbitmq_workspace + "//:BUILD.bats",
    )

    hex_pm_erlang_app(
        name = "cowboy",
        first_srcs = [
            "src/cowboy_stream.erl",
            "src/cowboy_middleware.erl",
            "src/cowboy_sub_protocol.erl",
        ],
        version = "2.8.0",
        sha256 = "4643e4fba74ac96d4d152c75803de6fad0b3fa5df354c71afdd6cbeeb15fac8a",
        deps = [
            "@cowlib//:erlang_app",
            "@ranch//:erlang_app",
        ],
    )

    hex_pm_erlang_app(
        name = "cowlib",
        version = "2.9.1",
        sha256 = "e4175dc240a70d996156160891e1c62238ede1729e45740bdd38064dad476170",
    )

    github_erlang_app(
        repo = "credentials-obfuscation",
        name = "credentials_obfuscation",
        org = "rabbitmq",
        sha256 = "a5cecd861334a8a5fb8c9b108a74c83ba0041653c53c523bb97f70dbefa30fe3",
        ref = "v2.4.0",
        version = "2.4.0",
    )

    github_erlang_app(
        name = "ct_helper",
        org = "extend",
    )

    hex_pm_erlang_app(
        name = "cuttlefish",
        version = "3.0.1",
        sha256 = "3feff3ae4ed1f0ca6df87ac89235068fbee9242ee85d2ac17fb1b8ce0e30f1a6",
    )

    hex_pm_erlang_app(
        name = "eetcd",
        version = "0.3.3",
        sha256 = "8fb280156ddd1b7b34d0f446c5711832385bff512c05378dcea8362f4f5060d6",
        runtime_deps = [
            "@gun//:erlang_app",
        ],
    )

    http_archive(
        name = "emqttc",
        urls = ["https://github.com/rabbitmq/emqttc/archive/remove-logging.zip"],
        strip_prefix = "emqttc-remove-logging",
        build_file_content = """load("@rules_erlang//:erlang_app.bzl", "erlang_app")

erlang_app(
    app_name = "emqttc",
    erlc_opts = [
        "+warn_export_all",
        "+warn_unused_import",
    ],
)
""",
    )

    hex_pm_erlang_app(
        name = "gen_batch_server",
        version = "0.8.4",
        sha256 = "ff6b0ed0f7be945f38b94ddd4784d128f35ff029c34dad6ca0c6cb17ab7bc9c4",
    )

    hex_pm_erlang_app(
        name = "goldrush",
        version = "0.1.9",
        sha256 = "99cb4128cffcb3227581e5d4d803d5413fa643f4eb96523f77d9e6937d994ceb",
    )

    hex_pm_erlang_app(
        name = "gun",
        version = "1.3.3",
        sha256 = "3106ce167f9c9723f849e4fb54ea4a4d814e3996ae243a1c828b256e749041e0",
        first_srcs = [
            "src/gun_content_handler.erl",
        ],
        runtime_deps = [
            "@cowlib//:erlang_app",
        ],
        erlc_opts = [
            "+debug_info",
            "+warn_export_vars",
            "+warn_shadow_vars",
            "+warn_obsolete_guard",
        ],
    )

    http_archive(
        name = "inet_tcp_proxy",
        build_file = rabbitmq_workspace + "//:BUILD.inet_tcp_proxy",
        strip_prefix = "inet_tcp_proxy-master",
        urls = ["https://github.com/rabbitmq/inet_tcp_proxy/archive/master.zip"],
    )

    github_erlang_app(
        name = "jose",
        repo = "erlang-jose",
        org = "potatosalad",
        ref = "2b1d66b5f4fbe33cb198149a8cb23895a2c877ea",
        version = "2b1d66b5f4fbe33cb198149a8cb23895a2c877ea",
        first_srcs = [
            "src/jose_block_encryptor.erl",
            "src/jwk/jose_jwk_use_enc.erl",
            "src/jwk/jose_jwk_use_sig.erl",
            "src/jwk/jose_jwk_oct.erl",
        ],
        sha256 = "7816f39d00655f2605cfac180755e97e268dba86c2f71037998ff63792ca727b",
    )

    hex_pm_erlang_app(
        name = "jsx",
        version = "3.1.0",
        sha256 = "0c5cc8fdc11b53cc25cf65ac6705ad39e54ecc56d1c22e4adb8f5a53fb9427f3",
    )

    hex_pm_erlang_app(
        name = "lager",
        first_srcs = [
            "src/lager_rotator_behaviour.erl",
        ],
        version = "3.8.2",
        sha256 = "73329ce700410b423f64aafc5f94583073904899098e4461f3558ed2980462ab",
        runtime_deps = [
            "@goldrush//:erlang_app",
        ],
        erlc_opts = [
            "+debug_info",
            "+warn_export_vars",
            "+warn_shadow_vars",
            "+warn_obsolete_guard",
        ],
    )

    github_erlang_app(
        name = "meck",
        org = "eproxus",
    )

    hex_pm_erlang_app(
        name = "observer_cli",
        version = "1.7.2",
        sha256 = "a1d280c112bb5443f09b63041d6c5dda39b40829db40b24fdf208e1b86dab353",
    )

    hex_pm_erlang_app(
        name = "prometheus",
        version = "4.8.1",
        sha256 = "6edfbe928d271c7f657a6f2c46258738086584bd6cae4a000b8b9a6009ba23a5",
        first_srcs = [
            "src/prometheus_collector.erl",
            "src/prometheus_format.erl",
            "src/prometheus_instrumenter.erl",
            "src/prometheus_metric.erl",
        ],
        deps = [
            "@quantile_estimator//:erlang_app",
        ],
    )

    github_erlang_app(
        name = "proper",
        first_srcs = [
            "src/vararg.erl",
            "src/proper_target.erl",
        ],
        org = "manopapad",
    )

    hex_pm_erlang_app(
        name = "quantile_estimator",
        version = "0.2.1",
        sha256 = "282a8a323ca2a845c9e6f787d166348f776c1d4a41ede63046d72d422e3da946",
        erlc_opts = [
            "+debug_info",
        ],
    )

    hex_pm_erlang_app(
        name = "ra",
        version = "1.1.8",
        sha256 = "d7e399f8a09c8420bc90953f3464127063e53cef39f27e0af452ec51ad26ea9e",
        first_srcs = [
            "src/ra_machine.erl",
            "src/ra_snapshot.erl",
        ],
        deps = [
            "@gen_batch_server//:erlang_app",
        ],
        runtime_deps = [
            "@aten//:erlang_app",
        ],
    )

    hex_archive(
        name = "ranch",
        version = "2.1.0",
        sha256 = "244ee3fa2a6175270d8e1fc59024fd9dbc76294a321057de8f803b1479e76916",
        build_file = rabbitmq_workspace + "//:BUILD.ranch",
    )

    hex_pm_erlang_app(
        name = "recon",
        version = "2.5.1",
        sha256 = "5721c6b6d50122d8f68cccac712caa1231f97894bab779eff5ff0f886cb44648",
    )

    hex_pm_erlang_app(
        name = "stdout_formatter",
        version = "0.2.4",
        sha256 = "51f1df921b0477275ea712763042155dbc74acc75d9648dbd54985c45c913b29",
    )

    github_erlang_app(
        name = "syslog",
        org = "schlagert",
        sha256 = "25abcfe2cc0745fc4ffb0d66d4a5868d343a0130c7a7ddcae03771326feae619",
        ref = "3.4.5",
        version = "3.4.5",
        first_srcs = [
            "src/syslog_logger.erl",
        ],
    )

    hex_pm_erlang_app(
        name = "sysmon_handler",
        version = "1.3.0",
        sha256 = "922cf0dd558b9fdb1326168373315b52ed6a790ba943f6dcbd9ee22a74cebdef",
    )

    new_git_repository(
        name = "trust_store_http",
        remote = "https://github.com/rabbitmq/trust-store-http.git",
        branch = "master",
        build_file = rabbitmq_workspace + "//:BUILD.trust_store_http",
    )
