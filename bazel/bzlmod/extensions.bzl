load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def _rbe(ctx):
    for mod in ctx.modules:
        for repo in mod.tags.git_repository:
            if repo.commit != "":
                git_repository(
                    name = "rbe",
                    commit = repo.commit,
                    remote = repo.remote,
                )
            if repo.tag != "":
                git_repository(
                    name = "rbe",
                    tag = repo.tag,
                    remote = repo.remote,
                )
            if repo.branch != "":
                git_repository(
                    name = "rbe",
                    branch = repo.branch,
                    remote = repo.remote,
                )

git_repository_tag = tag_class(attrs = {
    "remote": attr.string(),
    "branch": attr.string(),
    "tag": attr.string(),
    "commit": attr.string(),
})

rbe = module_extension(
    implementation = _rbe,
    tag_classes = {
        "git_repository": git_repository_tag,
    },
)
