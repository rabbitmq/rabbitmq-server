load(
    "@bazel_tools//tools/build_defs/repo:git.bzl",
    "git_repository",
    "new_git_repository",
)
load(
    "@rules_erlang//:hex_archive.bzl",
    "hex_archive",
)

HexPackage = provider(fields = [
    "module",
    "name",
    "pkg",
    "app_name",
    "version",
    "sha256",
    "build_file",
    "build_file_content",
    "patches",
    "patch_args",
    "patch_cmds",
    "testonly",
    "deps",
    "requirements",
    "f_fetch",
])

GitPackage = provider(fields = [
    "module",
    "name",
    "app_name",
    "version",
    "remote",
    "repository",
    "branch",
    "tag",
    "commit",
    "build_file",
    "build_file_content",
    "patch_cmds",
    "testonly",
    "f_fetch",
])

def log(ctx, msg):
    ctx.execute(["echo", "RULES_ELIXIR: " + msg], timeout = 1, quiet = False)

def hex_package(
        _ctx,
        module = None,
        dep = None):
    app_name = dep.pkg if dep.pkg != "" else dep.name

    return HexPackage(
        module = module,
        name = dep.name,
        pkg = dep.pkg,
        app_name = app_name,
        version = dep.version,
        sha256 = dep.sha256,
        build_file = dep.build_file,
        build_file_content = dep.build_file_content,
        patches = dep.patches,
        patch_args = dep.patch_args,
        patch_cmds = dep.patch_cmds,
        testonly = dep.testonly,
        f_fetch = _hex_package_repo,
    )

def _infer_app_name(remote):
    (_, _, repo) = remote.rpartition("/")
    if repo == remote:
        fail("Could not extract erlang app name from {}".format(remote))
    if not repo.endswith(".git"):
        fail("Could not extract erlang app name from {}".format(remote))
    return repo[0:-4]

def git_package(
        _ctx,
        module = None,
        dep = None):
    if dep.remote != "" and dep.repository != "":
        fail("'remote' and 'repository' are mutually exclusive options")

    if dep.repository != "":
        remote = "https://github.com/{}.git".format(dep.repository)
    elif dep.remote != "":
        remote = dep.remote
    else:
        fail("either 'remote' or 'repository' are required")

    if dep.name != "":
        name = dep.name
    else:
        name = _infer_app_name(remote)

    if dep.commit != "":
        version = dep.commit
    elif dep.tag != "":
        version = dep.tag
    else:
        version = dep.branch

    return GitPackage(
        module = module,
        name = name,
        app_name = name,
        version = version,
        remote = remote,
        branch = dep.branch,
        tag = dep.tag,
        commit = dep.commit,
        build_file = dep.build_file,
        build_file_content = dep.build_file_content,
        patch_cmds = dep.patch_cmds,
        testonly = dep.testonly,
        f_fetch = _git_package_repo,
    )

def _hex_package_repo(hex_package):
    if hex_package.build_file != None:
        hex_archive(
            name = hex_package.name,
            package_name = hex_package.app_name,
            version = hex_package.version,
            sha256 = hex_package.sha256,
            build_file = hex_package.build_file,
            patches = hex_package.patches,
            patch_args = hex_package.patch_args,
            patch_cmds = hex_package.patch_cmds,
        )
    elif hex_package.build_file_content != "":
        hex_archive(
            name = hex_package.name,
            package_name = hex_package.app_name,
            version = hex_package.version,
            sha256 = hex_package.sha256,
            build_file_content = hex_package.build_file_content,
            patches = hex_package.patches,
            patch_args = hex_package.patch_args,
            patch_cmds = hex_package.patch_cmds,
        )
    else:
        hex_archive(
            name = hex_package.name,
            package_name = hex_package.app_name,
            version = hex_package.version,
            sha256 = hex_package.sha256,
            build_file_content = DEFAULT_BUILD_FILE_CONTENT.format(
                app_name = hex_package.app_name,
                testonly = hex_package.testonly,
            ),
            patches = hex_package.patches,
            patch_args = hex_package.patch_args,
            patch_cmds = hex_package.patch_cmds,
        )

def _git_package_repo(git_package):
    if git_package.build_file != None:
        new_git_repository(
            name = git_package.name,
            remote = git_package.remote,
            branch = git_package.branch,
            tag = git_package.tag,
            commit = git_package.commit,
            build_file = git_package.build_file,
            patch_cmds = git_package.patch_cmds,
        )
    elif git_package.build_file_content != "":
        new_git_repository(
            name = git_package.name,
            remote = git_package.remote,
            branch = git_package.branch,
            tag = git_package.tag,
            commit = git_package.commit,
            build_file_content = git_package.build_file_content,
            patch_cmds = git_package.patch_cmds,
        )
    else:
        git_repository(
            name = git_package.name,
            remote = git_package.remote,
            branch = git_package.branch,
            tag = git_package.tag,
            commit = git_package.commit,
            build_file_content = DEFAULT_BUILD_FILE_CONTENT.format(
                app_name = git_package.app_name,
                testonly = git_package.testonly,
            ),
            patch_cmds = git_package.patch_cmds,
        )

DEFAULT_BUILD_FILE_CONTENT = """\
filegroup(
    name = "srcs",
    srcs = [
        "mix.exs",
    ] + glob([
        "LICENSE*",
        "lib/**/*",
    ]),
    visibility = ["//visibility:public"],
)
"""
