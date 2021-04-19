load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")
load("@bazel-erlang//:github.bzl", "github_bazel_erlang_lib")
load("@bazel-erlang//:hex_archive.bzl", "hex_archive")
load("@bazel-erlang//:hex_pm.bzl", "hex_pm_bazel_erlang_lib")
load("//:rabbitmq.bzl", "APP_VERSION")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    github_bazel_erlang_lib(
        name = "aten",
        org = "rabbitmq",
        sha256 = "27f6b2ec2e78027ea852a8ac6bcf49df4a599d5506a86dc9f0cb6b5d6e45989e",
        ref = "v0.5.6",
        version = "0.5.6",
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
        name = "cuttlefish",
        org = "Kyorai",
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

    github_bazel_erlang_lib(
        repo = "gen-batch-server",
        name = "gen_batch_server",
        org = "rabbitmq",
        sha256 = "9e9f2aa6ee8e3354f03a3f78283fde93bbe5b1d6f6732caa05d3e43efe02e42c",
        ref = "v0.8.4",
        version = "0.8.4",
    )

    hex_pm_bazel_erlang_lib(
        name = "goldrush",
        version = "0.1.9",
        sha256 = "99cb4128cffcb3227581e5d4d803d5413fa643f4eb96523f77d9e6937d994ceb",
    )

    http_archive(
        name = "inet_tcp_proxy",
        build_file = rabbitmq_workspace + "//:BUILD.inet_tcp_proxy",
        strip_prefix = "inet_tcp_proxy-master",
        urls = ["https://github.com/rabbitmq/inet_tcp_proxy/archive/master.zip"],
    )

    hex_pm_bazel_erlang_lib(
        name = "jose",
        version = "1.11.1",
        sha256 = "078f6c9fb3cd2f4cfafc972c814261a7d1e8d2b3685c0a76eb87e158efff1ac5",
        first_srcs = [
            "src/jose_block_encryptor.erl",
            "src/jwk/jose_jwk_use_enc.erl",
            "src/jwk/jose_jwk_use_sig.erl",
            "src/jwk/jose_jwk_oct.erl",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "jsx",
        version = "2.11.0",
        sha256 = "eed26a0d04d217f9eecefffb89714452556cf90eb38f290a27a4d45b9988f8c0",
        erlc_opts = [
            "+debug_info",
            "-Dmaps_support=1",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "lager",
        first_srcs = [
            "src/lager_rotator_behaviour.erl",
        ],
        version = "3.8.2",
        sha256 = "73329ce700410b423f64aafc5f94583073904899098e4461f3558ed2980462ab",
        runtime_deps = [
            "@goldrush//:bazel_erlang_lib",
        ],
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

    http_archive(
        name = "osiris",
        build_file = rabbitmq_workspace + "//:BUILD.osiris",
        strip_prefix = "osiris-master",
        urls = ["https://github.com/rabbitmq/osiris/archive/master.zip"],
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
        name = "ra",
        version = "1.1.8",
        sha256 = "d7e399f8a09c8420bc90953f3464127063e53cef39f27e0af452ec51ad26ea9e",
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

    hex_pm_bazel_erlang_lib(
        name = "ranch",
        first_srcs = [
            "src/ranch_transport.erl",
        ],
        version = "1.7.1",
        sha256 = "451d8527787df716d99dc36162fca05934915db0b6141bbdac2ea8d3c7afc7d7",
    )

    hex_pm_bazel_erlang_lib(
        name = "recon",
        version = "2.5.1",
        sha256 = "5721c6b6d50122d8f68cccac712caa1231f97894bab779eff5ff0f886cb44648",
    )

    hex_pm_bazel_erlang_lib(
        name = "stdout_formatter",
        version = "0.2.4",
        sha256 = "51f1df921b0477275ea712763042155dbc74acc75d9648dbd54985c45c913b29",
    )

    github_bazel_erlang_lib(
        name = "syslog",
        org = "schlagert",
        sha256 = "25abcfe2cc0745fc4ffb0d66d4a5868d343a0130c7a7ddcae03771326feae619",
        ref = "3.4.5",
        version = "3.4.5",
        first_srcs = [
            "src/syslog_logger.erl",
        ],
    )

    hex_pm_bazel_erlang_lib(
        name = "sysmon_handler",
        version = "1.3.0",
        sha256 = "922cf0dd558b9fdb1326168373315b52ed6a790ba943f6dcbd9ee22a74cebdef",
    )
