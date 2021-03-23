load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel-erlang//:github.bzl", "github_bazel_erlang_lib")
load("//:rabbitmq.bzl", "APP_VERSION")

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    github_bazel_erlang_lib(
        name = "aten",
        org = "rabbitmq",
        sha256 = "27f6b2ec2e78027ea852a8ac6bcf49df4a599d5506a86dc9f0cb6b5d6e45989e",
        ref = "v0.5.6",
        version = "0.5.6",
    )

    github_bazel_erlang_lib(
        name = "cowboy",
        first_srcs = [
            "src/cowboy_stream.erl",
            "src/cowboy_middleware.erl",
            "src/cowboy_sub_protocol.erl",
        ],
        org = "ninenines",
        sha256 = "c0248d7ab6e1f27f7fce2f6c52f7b418c76b970f5e3394333485b387d67f44cb",
        ref = "2.8.0",
        version = "2.8.0",
        deps = [
            "@cowlib//:bazel_erlang_lib",
            "@ranch//:bazel_erlang_lib",
        ],
    )

    github_bazel_erlang_lib(
        name = "cowlib",
        org = "ninenines",
        sha256 = "5a4f579015481e72c87187a46cf7517dd451bc45445ba49c7b5e09c74bfd3f9c",
        ref = "2.10.1",
        version = "2.10.1",
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

    github_bazel_erlang_lib(
        repo = "gen-batch-server",
        name = "gen_batch_server",
        org = "rabbitmq",
        sha256 = "9e9f2aa6ee8e3354f03a3f78283fde93bbe5b1d6f6732caa05d3e43efe02e42c",
        ref = "v0.8.4",
        version = "0.8.4",
    )

    github_bazel_erlang_lib(
        name = "goldrush",
        org = "DeadZen",
        sha256 = "9e82214dea7909f9068ae15ee1fbb13ccc622a4bb2b7cdb2c87b5dd8f9be3a6d",
        ref = "refs/tags/0.2.0",
        version = "0.2.0",
    )

    http_archive(
        name = "inet_tcp_proxy",
        build_file = rabbitmq_workspace + "//:BUILD.inet_tcp_proxy",
        strip_prefix = "inet_tcp_proxy-master",
        urls = ["https://github.com/rabbitmq/inet_tcp_proxy/archive/master.zip"],
    )

    github_bazel_erlang_lib(
        name = "jsx",
        org = "talentdeficit",
        sha256 = "7e9b051fcc6014b3e2dec45e61734fcea991f4043f1f4df4d806d39c4123ff6c",
        ref = "v3.0.0",
        version = "3.0.0",
    )

    github_bazel_erlang_lib(
        name = "lager",
        first_srcs = ["src/lager_rotator_behaviour.erl"],
        org = "erlang-lager",
        sha256 = "10da9c026c29b9d647353909a83b2ff769548de7308aa9e9138aa640527c83bc",
        ref = "3.8.1",
        version = "3.8.1",
    )

    github_bazel_erlang_lib(
        name = "meck",
        org = "eproxus",
        sha256 = "7bb57fbfca65c4cda7a8783ee11747db409a56cb201abe4311de161cf6d233af",
        ref = "0.9.0",
        version = "0.9.0",
    )

    github_bazel_erlang_lib(
        name = "observer_cli",
        org = "zhongwencool",
        sha256 = "b31d2fa5a9d6c3857180bee4a17d265945e0656ad66faf20594a3a8400b3a7e8",
        ref = "1.6.0",
        version = "1.6.0",
    )

    http_archive(
        name = "osiris",
        build_file = rabbitmq_workspace + "//:BUILD.osiris",
        strip_prefix = "osiris-master",
        urls = ["https://github.com/rabbitmq/osiris/archive/master.zip"],
    )

    github_bazel_erlang_lib(
        name = "proper",
        first_srcs = ["src/vararg.erl"],
        org = "proper-testing",
        sha256 = "b2fb969604ae71b0ea0ab7d8cedca78cef5f46181aba433c1ea493db0eaf81c6",
        ref = "v1.3",
        version = "1.3",
    )

    http_archive(
        name = "ra",
        build_file = rabbitmq_workspace + "//:BUILD.ra",
        strip_prefix = "ra-master",
        urls = ["https://github.com/rabbitmq/ra/archive/master.zip"],
    )

    github_bazel_erlang_lib(
        name = "ranch",
        first_srcs = [
            "src/ranch_transport.erl",
        ],
        org = "ninenines",
        sha256 = "3c4d8c325326acf0ead8db1353fc3f71739e7c90c12959c0f921df495312ab46",
        ref = "2.0.0",
        version = "2.0.0",
    )

    github_bazel_erlang_lib(
        name = "recon",
        org = "ferd",
        sha256 = "299701100e0e9f3845cf9a02d51b7c554b40342e014df2b8de7a465f5599d3af",
        ref = "2.5.1",
        version = "2.5.1",
    )

    github_bazel_erlang_lib(
        name = "stdout_formatter",
        org = "rabbitmq",
        sha256 = "9eb27075c25006f86da0168597d7e4914ecc41e2493c0b3cba14c07f4be53267",
        ref = "v0.2.4",
        version = "0.2.4",
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

    github_bazel_erlang_lib(
        repo = "sysmon-handler",
        name = "sysmon_handler",
        org = "rabbitmq",
        sha256 = "0fd50afe194dd071e7afc31c4bdfdcc789652edc72c2defff1e5206f5d4f43ee",
        ref = "v1.3.0",
        version = "1.3.0",
    )
