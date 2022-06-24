load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load(
    ":secondary_umbrella.bzl",
    fetch_secondary_umbrella = "secondary_umbrella",
)

def _rbe(ctx):
    rbe_repo_props = []
    for mod in ctx.modules:
        for repo in mod.tags.git_repository:
            props = {"remote": repo.remote}
            if repo.commit != "":
                props["commit"] = repo.commit
            if repo.tag != "":
                props["tag"] = repo.tag
            if repo.branch != "":
                props["branch"] = repo.branch
            if not props in rbe_repo_props:
                rbe_repo_props.append(props)

    if len(rbe_repo_props) > 1:
        fail("Multiple definitions for @rbe exist: {}".format(rbe_repo_props))

    if len(rbe_repo_props) > 0:
        git_repository(
            name = "rbe",
            **rbe_repo_props[0]
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

def _secondary_umbrella(ctx):
    fetch_secondary_umbrella()

secondary_umbrella = module_extension(
    implementation = _secondary_umbrella,
)
