load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")
load("@bazel-erlang//:github.bzl", "github_bazel_erlang_lib")
load("@bazel-erlang//:hex_archive.bzl", "hex_archive")
load("@bazel-erlang//:hex_pm.bzl", "hex_pm_bazel_erlang_lib")
load("//:rabbitmq.bzl", "APP_VERSION")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    hex_pm_bazel_erlang_lib(
        name = "accept",
        version = "0.3.5",
        sha256 = "11b18c220bcc2eab63b5470c038ef10eb6783bcb1fcdb11aa4137defa5ac1bb8",
    )

    github_bazel_erlang_lib(
        name = "aten",
        org = "rabbitmq",
        sha256 = "27f6b2ec2e78027ea852a8ac6bcf49df4a599d5506a86dc9f0cb6b5d6e45989e",
        ref = "v0.5.6",
        version = "0.5.6",
    )

    hex_pm_bazel_erlang_lib(
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

    hex_pm_bazel_erlang_lib(
        name = "cowboy",
        first_srcs = [
            "src/cowboy_stream.erl",
            "src/cowboy_middleware.erl",
            "src/cowboy_sub_protocol.erl",
        ],
        version = "2.8.0",
        sha256 = "4643e4fba74ac96d4d152c75803de6fad0b3fa5df354c71afdd6cbeeb15fac8a",
        deps = [
            "@cowlib//:bazel_erlang_lib",
            "@ranch//:bazel_erlang_lib",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "cowlib",
        version = "2.9.1",
        sha256 = "e4175dc240a70d996156160891e1c62238ede1729e45740bdd38064dad476170",
    )

    github_bazel_erlang_lib(
        repo = "credentials-obfuscation",
        name = "credentials_obfuscation",
        org = "rabbitmq",
        sha256 = "a5cecd861334a8a5fb8c9b108a74c83ba0041653c53c523bb97f70dbefa30fe3",
        ref = "v2.4.0",
        version = "2.4.0",
    )

    github_bazel_erlang_lib(
        name = "ct_helper",
        org = "extend",
    )

    hex_pm_bazel_erlang_lib(
        name = "cuttlefish",
        version = "3.0.1",
        sha256 = "3feff3ae4ed1f0ca6df87ac89235068fbee9242ee85d2ac17fb1b8ce0e30f1a6",
    )

    hex_pm_bazel_erlang_lib(
        name = "eetcd",
        version = "0.3.3",
        sha256 = "8fb280156ddd1b7b34d0f446c5711832385bff512c05378dcea8362f4f5060d6",
        runtime_deps = [
            "@gun//:bazel_erlang_lib",
        ],
    )

    http_archive(
        name = "emqttc",
        urls = ["https://github.com/rabbitmq/emqttc/archive/remove-logging.zip"],
        strip_prefix = "emqttc-remove-logging",
        build_file_content = """load("@bazel-erlang//:bazel_erlang_lib.bzl", "erlang_lib")

erlang_lib(
    app_name = "emqttc",
    erlc_opts = [
        "+warn_export_all",
        "+warn_unused_import",
    ],
)
""",
    )

    hex_pm_bazel_erlang_lib(
        name = "enough",
        version = "0.1.0",
        sha256 = "0460c7abda5f5e0ea592b12bc6976b8a5c4b96e42f332059cd396525374bf9a1",
    )

    hex_pm_bazel_erlang_lib(
        name = "gen_batch_server",
        version = "0.8.6",
        sha256 = "b78679349168f27d7047f3283c9d766760b234d98c762aca9a1907f4ee3fd406",
    )

    hex_pm_bazel_erlang_lib(
        name = "gun",
        version = "1.3.3",
        sha256 = "3106ce167f9c9723f849e4fb54ea4a4d814e3996ae243a1c828b256e749041e0",
        first_srcs = [
            "src/gun_content_handler.erl",
        ],
        runtime_deps = [
            "@cowlib//:bazel_erlang_lib",
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

    github_bazel_erlang_lib(
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

    hex_pm_bazel_erlang_lib(
        name = "jsx",
        version = "3.1.0",
        sha256 = "0c5cc8fdc11b53cc25cf65ac6705ad39e54ecc56d1c22e4adb8f5a53fb9427f3",
    )

    github_bazel_erlang_lib(
        name = "meck",
        org = "eproxus",
    )

    hex_pm_bazel_erlang_lib(
        name = "observer_cli",
        version = "1.6.1",
        sha256 = "3418e319764b9dff1f469e43cbdffd7fd54ea47cbf765027c557abd146a19fb3",
    )

    github_bazel_erlang_lib(
        name = "osiris",
        org = "rabbitmq",
        ref = "v1.0.0",
        version = "1.0.0",
        build_file = rabbitmq_workspace + "//:BUILD.osiris",
    )

    hex_pm_bazel_erlang_lib(
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
            "@quantile_estimator//:bazel_erlang_lib",
        ],
    )

    github_bazel_erlang_lib(
        name = "proper",
        first_srcs = [
            "src/vararg.erl",
            "src/proper_target.erl",
        ],
        org = "manopapad",
    )

    hex_pm_bazel_erlang_lib(
        name = "quantile_estimator",
        version = "0.2.1",
        erlc_opts = [
            "+debug_info",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "ra",
        version = "2.0.0",
        sha256 = "6d29d58040279ed214d3365dfdc81a7feca7531fe24f3425f6dc8fc3a0d9a519",
        first_srcs = [
            "src/ra_machine.erl",
            "src/ra_snapshot.erl",
        ],
        deps = [
            "@gen_batch_server//:bazel_erlang_lib",
        ],
        runtime_deps = [
            "@aten//:bazel_erlang_lib",
        ],
    )

    hex_archive(
        name = "ranch",
        version = "2.0.0",
        sha256 = "c20a4840c7d6623c19812d3a7c828b2f1bd153ef0f124cb69c54fe51d8a42ae0",
        build_file = rabbitmq_workspace + "//:BUILD.ranch",
    )

    hex_pm_bazel_erlang_lib(
        name = "recon",
        version = "2.5.1",
        sha256 = "5721c6b6d50122d8f68cccac712caa1231f97894bab779eff5ff0f886cb44648",
    )

    github_bazel_erlang_lib(
        name = "seshat",
        org = "rabbitmq",
        ref = "0.1.0",
        version = "0.1.0",
        extra_apps = [
            "sasl",
            "crypto",
        ],
        sha256 = "fd20039322eabed814d0dfe75743652846007ec93faae3e141c9602c21152b14",
    )

    hex_pm_bazel_erlang_lib(
        name = "stdout_formatter",
        version = "0.2.4",
        sha256 = "51f1df921b0477275ea712763042155dbc74acc75d9648dbd54985c45c913b29",
    )

    github_bazel_erlang_lib(
        name = "syslog",
        org = "schlagert",
        sha256 = "01c31c31d4d28e564da0660bdb69725ba37173fca5b3228829b8f3f416f9e486",
        ref = "4.0.0",
        version = "4.0.0",
        first_srcs = [
            "src/syslog_logger.erl",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "sysmon_handler",
        version = "1.3.0",
        sha256 = "922cf0dd558b9fdb1326168373315b52ed6a790ba943f6dcbd9ee22a74cebdef",
    )

    hex_pm_bazel_erlang_lib(
        name = "systemd",
        version = "0.6.1",
        sha256 = "8ec5ed610a5507071cdb7423e663e2452a747a624bb8a58582acd9491ccad233",
        deps = [
            "@enough//:bazel_erlang_lib",
        ],
    )

    new_git_repository(
        name = "trust_store_http",
        remote = "https://github.com/rabbitmq/trust-store-http.git",
        branch = "master",
        build_file = rabbitmq_workspace + "//:BUILD.trust_store_http",
    )
