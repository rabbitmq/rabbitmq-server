load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//:rabbitmq.bzl", "APP_VERSION")

def github_bazel_erlang_lib(name, org="rabbitmq", version=APP_VERSION, tag=None, sha256=None, app_name=None, **kwargs):
    if not ("build_file" in kwargs.keys() or "build_file_content" in kwargs.keys()):
        kwargs.update(build_file_content=_BUILD_FILE_TEMPLATE.format(
            app_name=app_name if app_name != None else name,
            version=version,
        ))
    
    tag = "v{}".format(version) if tag == None else tag

    http_archive(
        name = name,
        urls = ["https://github.com/{}/{}/archive/{}.zip".format(org, name, tag)],
        sha256 = sha256,
        strip_prefix = "{}-{}".format(name, version),
        **kwargs,
    )

_BUILD_FILE_TEMPLATE = """
load("@//bazel_erlang:bazel_erlang_lib.bzl", "app_file", "erlc", "bazel_erlang_lib")

app_file(
    name = "app_file",
    app_name = "{app_name}",
    app_version = "{version}",
    app_src = glob(["src/{app_name}.app.src"]),
    modules = [":beam_files"],
)

erlc(
    name = "beam_files",
    hdrs = glob(["include/*.hrl", "src/*.hrl"]),
    srcs = glob(["src/*.erl"]),
    dest = "ebin",
)

bazel_erlang_lib(
    name = "bazel_erlang_lib",
    app_name = "{app_name}",
    app_version = "{version}",
    hdrs = glob(["include/*.hrl"]),
    beam = [":beam_files"],
    visibility = ["//visibility:public"],
)
"""