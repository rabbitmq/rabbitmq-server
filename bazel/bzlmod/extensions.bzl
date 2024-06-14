load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    ":secondary_umbrella.bzl",
    fetch_secondary_umbrella = "secondary_umbrella",
)

def _secondary_umbrella(_ctx):
    fetch_secondary_umbrella()

secondary_umbrella = module_extension(
    implementation = _secondary_umbrella,
)

def _hex(_ctx):
    http_archive(
        name = "hex",
        sha256 = "0e3e3290d0fcbdc6bb0526b73ca174d68dcff4d53ee86015c49ad0493e39ee65",
        strip_prefix = "hex-2.0.5",
        urls = ["https://github.com/hexpm/hex/archive/refs/tags/v2.0.5.zip"],
        build_file_content = """\
load(
    "@rabbitmq-server//bazel/elixir:mix_archive_build.bzl",
    "mix_archive_build",
)

mix_archive_build(
    name = "archive",
    srcs = [
        "mix.exs",
    ] + glob([
        "lib/**/*",
    ]),
    out = "hex.ez",
    visibility = ["//visibility:public"],
)
""",
    )

hex = module_extension(
    implementation = _hex,
)
