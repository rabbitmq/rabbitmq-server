filegroup(
    name = "bin_dir",
    srcs = glob(["bin/**/*", "libexec/**/*"]),
    visibility = ["//visibility:public"],
)
