load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//:rabbitmq.bzl", "APP_VERSION")

def github_bazel_erlang_lib(name, org = "rabbitmq", version = APP_VERSION, tag = None, sha256 = None, app_name = None, first_srcs = [], deps = [], **kwargs):
    if not ("build_file" in kwargs.keys() or "build_file_content" in kwargs.keys()):
        kwargs.update(build_file_content = _BUILD_FILE_TEMPLATE.format(
            app_name = app_name if app_name != None else name,
            version = version,
            first_srcs = first_srcs,
            deps = deps,
        ))

    tag = "v{}".format(version) if tag == None else tag

    http_archive(
        name = name,
        urls = ["https://github.com/{}/{}/archive/{}.zip".format(org, name, tag)],
        sha256 = sha256,
        strip_prefix = "{}-{}".format(name, version),
        **kwargs
    )

_BUILD_FILE_TEMPLATE = """
load("@bazel-erlang//:bazel_erlang_lib.bzl", "erlang_lib")

erlang_lib(
    app_name = "{app_name}",
    app_version = "{version}",
    first_srcs = {first_srcs},
    deps = {deps},
)
"""

def rabbitmq_external_deps(rabbitmq_workspace = "@rabbitmq-server"):
    github_bazel_erlang_lib(
        name = "aten",
        org = "rabbitmq",
        sha256 = "27f6b2ec2e78027ea852a8ac6bcf49df4a599d5506a86dc9f0cb6b5d6e45989e",
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
        tag = "2.8.0",
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
        tag = "2.10.1",
        version = "2.10.1",
    )

    github_bazel_erlang_lib(
        name = "credentials-obfuscation",
        app_name = "credentials_obfuscation",
        org = "rabbitmq",
        sha256 = "20890287379005d277465d9a705b3c79e906cb19825b422350c519bbc7c6c273",
        version = "2.3.0",
    )

    github_bazel_erlang_lib(
        name = "cuttlefish",
        build_file = rabbitmq_workspace + "//:BUILD.cuttlefish",
        org = "Kyorai",
        sha256 = "178d2284b369fe92312727e3a460916da76e452e4ea91257afd36d8265783a1e",
        version = "2.5.0",
    )

    github_bazel_erlang_lib(
        name = "gen-batch-server",
        app_name = "gen_batch_server",
        org = "rabbitmq",
        sha256 = "9e9f2aa6ee8e3354f03a3f78283fde93bbe5b1d6f6732caa05d3e43efe02e42c",
        version = "0.8.4",
    )

    github_bazel_erlang_lib(
        name = "goldrush",
        org = "DeadZen",
        sha256 = "9e82214dea7909f9068ae15ee1fbb13ccc622a4bb2b7cdb2c87b5dd8f9be3a6d",
        tag = "0.2.0",
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
        version = "3.0.0",
    )

    github_bazel_erlang_lib(
        name = "lager",
        first_srcs = ["src/lager_rotator_behaviour.erl"],
        org = "erlang-lager",
        sha256 = "10da9c026c29b9d647353909a83b2ff769548de7308aa9e9138aa640527c83bc",
        tag = "3.8.1",
        version = "3.8.1",
    )

    github_bazel_erlang_lib(
        name = "meck",
        org = "eproxus",
        sha256 = "7bb57fbfca65c4cda7a8783ee11747db409a56cb201abe4311de161cf6d233af",
        tag = "0.9.0",
        version = "0.9.0",
    )

    github_bazel_erlang_lib(
        name = "observer_cli",
        org = "zhongwencool",
        sha256 = "b31d2fa5a9d6c3857180bee4a17d265945e0656ad66faf20594a3a8400b3a7e8",
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
        build_file = rabbitmq_workspace + "//:BUILD.ranch",
        org = "ninenines",
        sha256 = "5607ea3b61fe8e715a1e711c5ce9000c8f16d4c2401c11318b76f65d11bf22d4",
        tag = "1.7.1",
        version = "1.7.1",
    )

    github_bazel_erlang_lib(
        name = "recon",
        org = "ferd",
        sha256 = "299701100e0e9f3845cf9a02d51b7c554b40342e014df2b8de7a465f5599d3af",
        tag = "2.5.1",
        version = "2.5.1",
    )

    github_bazel_erlang_lib(
        name = "stdout_formatter",
        sha256 = "9eb27075c25006f86da0168597d7e4914ecc41e2493c0b3cba14c07f4be53267",
        version = "0.2.4",
    )

    github_bazel_erlang_lib(
        name = "sysmon-handler",
        app_name = "sysmon_handler",
        sha256 = "0fd50afe194dd071e7afc31c4bdfdcc789652edc72c2defff1e5206f5d4f43ee",
        version = "1.3.0",
    )
