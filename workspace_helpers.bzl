load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@rules_erlang//:github.bzl", "github_erlang_app")
load("@rules_erlang//:hex_archive.bzl", "hex_archive")
load("@rules_erlang//:hex_pm.bzl", "hex_pm_erlang_app")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    hex_pm_erlang_app(
        name = "accept",
        version = "0.3.5",
        sha256 = "11b18c220bcc2eab63b5470c038ef10eb6783bcb1fcdb11aa4137defa5ac1bb8",
    )

    hex_pm_erlang_app(
        name = "aten",
        sha256 = "64d40a8cf0ddfea4e13af00b7327f0925147f83612d0627d9506cbffe90c13ef",
        version = "0.5.8",
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

    hex_pm_erlang_app(
        name = "credentials_obfuscation",
        version = "3.2.0",
        sha256 = "fe8ece91a1ba6c8a08eb1063cfd5b063a723c5fe29a1fad6b7cbd76cb18d2eeb",
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
        version = "0.3.6",
        sha256 = "66493bfd6698c1b6baa49679034c3def071ff329961ca1aa7b1dee061c2809af",
        runtime_deps = [
            "@gun//:erlang_app",
        ],
    )

    hex_pm_erlang_app(
        name = "enough",
        version = "0.1.0",
        sha256 = "0460c7abda5f5e0ea592b12bc6976b8a5c4b96e42f332059cd396525374bf9a1",
    )

    hex_pm_erlang_app(
        name = "gen_batch_server",
        version = "0.8.7",
        sha256 = "94a49a528486298b009d2a1b452132c0a0d68b3e89d17d3764cb1ec879b7557a",
    )

    hex_pm_erlang_app(
        name = "gun",
        version = "1.3.3",
        sha256 = "3106ce167f9c9723f849e4fb54ea4a4d814e3996ae243a1c828b256e749041e0",
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
        name = "inet_tcp_proxy_dist",
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
        sha256 = "7816f39d00655f2605cfac180755e97e268dba86c2f71037998ff63792ca727b",
        build_file = rabbitmq_workspace + "//:BUILD.jose",
    )

    hex_pm_erlang_app(
        name = "thoas",
        version = "0.3.0",
        sha256 = "b8e1f8c8fad317c0b75239a9234cb093de1fb8be7ba3e41433ff80a0b3353973",
    )

    github_erlang_app(
        name = "meck",
        org = "eproxus",
    )

    hex_pm_erlang_app(
        name = "observer_cli",
        version = "1.7.3",
        sha256 = "a41b6d3e11a3444e063e09cc225f7f3e631ce14019e5fbcaebfda89b1bd788ea",
    )

    git_repository(
        name = "osiris",
        tag = "v1.4.2",
        remote = "https://github.com/rabbitmq/osiris.git",
    )

    hex_pm_erlang_app(
        name = "prometheus",
        version = "4.9.1",
        deps = [
            "@quantile_estimator//:erlang_app",
        ],
        sha256 = "d75e80d7b2c1be6bf296e211e806e939ae3d9e0428f45b4caad1817f028213d3",
    )

    github_erlang_app(
        name = "proper",
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

    hex_pm_erlang_app(
        name = "ra",
        version = "2.4.5",
        sha256 = "9315fb67d1e1ae0c83cb7b0481851c90e5c0fe65a7040359ddbd2fb179ce01df",
        build_file_content = """load("@rules_erlang//:erlang_app.bzl", "erlang_app")

NAME = "ra"

EXTRA_APPS = [
    "sasl",
    "crypto",
]

DEPS = [
    "@gen_batch_server//:erlang_app",
]

RUNTIME_DEPS = [
    "@aten//:erlang_app",
    "@seshat//:erlang_app",
]

erlang_app(
    app_name = NAME,
    extra_apps = EXTRA_APPS,
    runtime_deps = RUNTIME_DEPS,
    deps = DEPS,
)
""",
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
        erlc_opts = [
            "+deterministic",
            "+debug_info",
        ],
    )

    hex_pm_erlang_app(
        name = "seshat",
        version = "0.4.0",
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

    github_erlang_app(
        name = "emqtt",
        org = "emqx",
        repo = "emqtt",
        version = "1.7.0-rc.2",
        ref = "1.7.0-rc.2",
        build_file_content = """load("@rules_erlang//:erlang_app.bzl", "erlang_app")

erlang_app(
    app_name = "emqtt",
    erlc_opts = [
        "+deterministic",
        "+debug_info",
        "-DBUILD_WITHOUT_QUIC",
    ],
)
""",
    )

RA_INJECT_GIT_VERSION = """
VERSION=$(git rev-parse HEAD)
echo "Injecting ${VERSION} into ra.app.src..."
sed -i"_orig" "/vsn,/ s/2\\.[0-9]\\.[0-9]/${VERSION}/" src/ra.app.src
"""
