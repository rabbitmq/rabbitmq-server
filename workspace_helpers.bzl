load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
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
        sha256 = "f27453bfa270538e1b48a9111206847e19a5ad51b4ded4f03fcb0184fbb555be",
        ref = "v0.5.7",
        version = "0.5.7",
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
        name = "enough",
        version = "0.1.0",
        sha256 = "0460c7abda5f5e0ea592b12bc6976b8a5c4b96e42f332059cd396525374bf9a1",
    )

    hex_pm_erlang_app(
        name = "gen_batch_server",
        version = "0.8.7",
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

    github_erlang_app(
        name = "meck",
        org = "eproxus",
    )

    hex_pm_erlang_app(
        name = "observer_cli",
        version = "1.7.2",
        sha256 = "a1d280c112bb5443f09b63041d6c5dda39b40829db40b24fdf208e1b86dab353",
    )

    git_repository(
        name = "osiris",
        branch = "main",
        remote = "https://github.com/rabbitmq/osiris.git",
    )

    github_erlang_app(
        name = "prometheus",
        repo = "prometheus.erl",
        org = "deadtrickster",
        ref = "06425c21a39c1564164f1cc3fe5bdfa8b23b1f78",
        version = "06425c21a39c1564164f1cc3fe5bdfa8b23b1f78",
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
        erlc_opts = [
            "+debug_info",
        ],
        sha256 = "282a8a323ca2a845c9e6f787d166348f776c1d4a41ede63046d72d422e3da946",
    )

    git_repository(
        name = "ra",
        branch = "sparse-read",
        remote = "https://github.com/rabbitmq/ra.git",
    )

    hex_archive(
        name = "ranch",
        version = "2.1.0",
        sha256 = "244ee3fa2a6175270d8e1fc59024fd9dbc76294a321057de8f803b1479e76916",
        build_file = rabbitmq_workspace + "//:BUILD.ranch",
    )

    hex_pm_erlang_app(
        name = "recon",
        version = "2.5.2",
        sha256 = "2c7523c8dee91dff41f6b3d63cba2bd49eb6d2fe5bf1eec0df7f87eb5e230e1c",
    )

    hex_pm_erlang_app(
        name = "redbug",
        version = "2.0.7",
        sha256 = "3624feb7a4b78fd9ae0e66cc3158fe7422770ad6987a1ebf8df4d3303b1c4b0c",
    )

    github_erlang_app(
        name = "seshat",
        org = "rabbitmq",
        ref = "main",
        version = "main",
        extra_apps = [
            "sasl",
            "crypto",
        ],
    )

    hex_pm_erlang_app(
        name = "stdout_formatter",
        version = "0.2.4",
        sha256 = "51f1df921b0477275ea712763042155dbc74acc75d9648dbd54985c45c913b29",
    )

    github_erlang_app(
        name = "syslog",
        org = "schlagert",
        sha256 = "01c31c31d4d28e564da0660bdb69725ba37173fca5b3228829b8f3f416f9e486",
        ref = "4.0.0",
        version = "4.0.0",
        first_srcs = [
            "src/syslog_logger.erl",
        ],
    )

    hex_pm_erlang_app(
        name = "sysmon_handler",
        version = "1.3.0",
        sha256 = "922cf0dd558b9fdb1326168373315b52ed6a790ba943f6dcbd9ee22a74cebdef",
    )

    hex_pm_erlang_app(
        name = "systemd",
        version = "0.6.1",
        sha256 = "8ec5ed610a5507071cdb7423e663e2452a747a624bb8a58582acd9491ccad233",
        deps = [
            "@enough//:erlang_app",
        ],
    )

    new_git_repository(
        name = "trust_store_http",
        remote = "https://github.com/rabbitmq/trust-store-http.git",
        branch = "master",
        build_file = rabbitmq_workspace + "//:BUILD.trust_store_http",
    )
