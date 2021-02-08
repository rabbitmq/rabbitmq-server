load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//:rabbitmq.bzl", "APP_VERSION")

def github_bazel_erlang_lib(name, org="rabbitmq", version=APP_VERSION, tag=None, sha256=None, app_name=None, first_srcs=[], deps=[], **kwargs):
    if not ("build_file" in kwargs.keys() or "build_file_content" in kwargs.keys()):
        kwargs.update(build_file_content=_BUILD_FILE_TEMPLATE.format(
            app_name=app_name if app_name != None else name,
            version=version,
            first_srcs=first_srcs,
            deps=deps,
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
load("@bazel-erlang//:bazel_erlang_lib.bzl", "erlang_lib")

erlang_lib(
    app_name = "{app_name}",
    app_version = "{version}",
    first_srcs = {first_srcs},
    deps = {deps},
)
"""