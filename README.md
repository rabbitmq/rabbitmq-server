# RabbitMQ server releases

This repository provides scripts and Makefiles we use to create RabbitMQ
server releases. It is organized in the following way:
* The top-level `Makefile` manages the source archive.
* There is a subdirectory inside `packaging` for each type of package we
 support.

## TL;DR

* To create a source archive and all supported packages:

    ```
    make packages
    ```

* To create a source archive and all supported packages, with a given version:

    ```
    make packages PROJECT_VERSION=3.8.1-rc.1
    ```

* To create all suported packages from an existing source archive:

    ```
    make -C packaging SOURCE_DIST_FILE=/path/to/rabbitmq-server-3.8.1-rc.1.tar.xz
    ```

The standalone package is different because it embeds the build
platform's Erlang copy. Thus on Linux for instance, only the
`linux-x86_64` standalone package will be built. To build the OS X
standalone package, you need to run the following command on an OS X
build host:

```
make package-standalone-macosx
# or
make -C packaging package-standalone-macosx SOURCE_DIST_FILE=/path/to/rabbitmq-server-3.8.1-rc.1.tar.xz
```

The instructions in the [`PKG_LINUX.md`](PKG_LINUX.md) document include a
script to install the necessary pre-requisites for building package archives as
well as `deb` and `rpm` packages.

## Source archive

### How to create it

The source archive is created with the following command:
```
make source-dist
```

It uses Erlang.mk's `PROJECT_VERSION` variable to set the version of the
source archive. If the variable is unset, Erlang.mk computes a value
based on the last tag and the current HEAD.

Here is an example with an explicit version:
```
make source-dist PROJECT_VERSION=3.8.1-rc.1
```

The version is automatically propagated to the broker and plugins so
they all advertise the same version.

The result is then available in the `PACKAGES` subdirectory. You can
override the output directory with the `PACKAGES_DIR` variable:
```
make source-dist PROJDCT_VERSION=3.8.1-rc.1 \
  PACKAGES_DIR=/tmp
```

By default, two archives are produced:
* a `tar.xz` file;
* a `zip` file.

You can ask for more/different types by specifying the
`SOURCE_DIST_SUFFIXES` variable:
```
make source-dist PROJECT_VERSION=3.8.1-rc.1 \
  SOURCE_DIST_SUFFIXES='tar.xz tar.gz'
```

Supported archive types are:
* `tar.bz2`;
* `tar.gz`;
* `tar.xz`;
* `zip`.

### What is included

The source archive includes the broker and a set of plugins. The default
list of plugins is in the `plugins.mk` file.

You can override this list by setting the `PLUGINS` variable to the list
you want:
```
make source-dist PROJECT_VERSION=3.8.1-rc.1 \
  PLUGINS='rabbitmq_shovel rabbitmq_rabbitmq_shovel_management'
```

Dependencies are automatically included.

## Packages

Packages can be built with an existing source archive or create the
source archive automatically.

If you want to use an existing archive, use `packaging/Makefile`:
```
make -C packaging package-$type \
  SOURCE_DIST_FILE=/path/to/rabbitmq-server-$version.tar.xz \
  ...
```

This has the following rules:
* The archive must be a `tar.xz` file.
* It can automatically take the only archive available under `PACKAGES`.
 However, if there is none or multiple archive, you must specify the
 `SOURCE_DIST_FILE` variable.

If you want the source archive to be created automatically, use the
top-level `Makefile`:
```
make package-$type PROJECT_VERSION=3.8.1-rc.1 ...
```

Packages are written to `PACKAGES_DIR`, like the source archive.

Each package type is further described separately because most of them
have versioning specificities.

### `generic-unix` package

To create it:
```
make package-generic-unix
```

There is no package revision, only the project version and no
restriction on it.

`packaging/generic-unix/Makefile` tries to determine the version based
on the source archive filename. If it fails, you can specify the version
with the `VERSION` variable:
```
make -C packaging package-generic-unix \
  SOURCE_DIST_FILE=rabbitmq-server.tar.xz \
  VERSION=3.8.1-rc.1
```

### Debian package

To create it:
```
make package-deb
```

The package may have a different versioning than the project and may
include an additional package revision. In particular, the package
version can't have any `-` characters.

`packaging/debs/Debian/Makefile` tries to determine the version based
on the source archive filename. If it fails, you can specify the version
with the `VERSION` variable:
```
make -C packaging package-deb \
  SOURCE_DIST_FILE=rabbitmq-server.tar.xz \
  VERSION=3.8.1-rc.1
```

By default, the package version is converted from `VERSION` with
all `-` characters replaced by `~` (eg. `3.8.1~rc.1` in the example
above). If you want to override that conversion, you can specify the
`DEBIAN_VERSION` variable:
```
make -C packaging package-deb \
  SOURCE_DIST_FILE=rabbitmq-server.tar.xz \
  VERSION=3.8.1-rc.1
  DEBIAN_VERSION=3.8.1~rc.1
```

### RPM package

We support RedHat and OpenSUSE RPM packages and both are created by default:

To create them:
```
make package-rpm
```

You can create a single one with:
```
make package-rpm-fedora
make package-rpm-suse
```

RPM packages have the same restrictions as Debian packages and use the
same default version conversion. To override the converted version, use
the `RPM_VERSION` variable. See the "Debian package" section above for
more details.

`packaging/RPMS/Fedora/Makefile`, which handles both RedHar and OpenSUSE
flavors, accepts the `RPM_OS` variable to set the flavor. It can be:
* `fedora`;
* `suse`.

### Windows package

We create two artefacts:

* a Zip archive, resembling the `generic-unix` package;
* an installer.

To create them:

```
make package-windows
```

To create them separately:

```
make -C packaging/windows     # the Zip archive
make -C packaging/windows-exe # the installer
```

The Zip archive has no package revision, only the project version and no
restriction on it. It supports the same `VERSION` as the `generic-unix`
package.

The installer requires a *product version* which must be 4 integers
separated by `.` characters. Furthermore, unlike other packages, this
one requires the Zip archive as its input, not the source archive.

So you need to built the Zip archive first, then the installer. You can
specify the path to the Zip archive using the `ZIP` variable:

```
make -C packaging/windows-exe ZIP=/path/to/rabbitmq-server-windows.zip
```

By default, the *product version* is the project version where
everything following the third integer was replaced by `.0`. Thus it's
only fine if the version is a semver-based version (eg. 3.8.1-pre.3 or
3.8.2). If the version doesn't conform to that, you need to set the
`PRODUCT_VERSION` variable:

```
make package-windows PROJECT_VERSION=3.8.1-rc.1 PRODUCT_VERSION=3.8.1.0
```

To build the Windows package using a Windows machine, follow the
instructions in [`PKG_WINDOWS.md`](PKG_WINDOWS.md).

### Standalone package

This is the equivalent of the `generic-unix` package with Erlang
embbeded.

To create it:
```
make -C packaging/standalone SOURCE_DIST_FILE=... VERSION=...
```

There is no package revision, only the project version and no
restriction on it.

Unlike other packages, the top-level `Makefile` and `packaging/Makefile`
provide targets to build the standalone package for specific platforms:
```
make package-standalone-macosx
make package-standalone-linux-x86_64
make package-standalone-freebsd-x86_64
```

Cross-build isn't supported so using those targets on incompatible
platforms is a no-op.

If you want to build a standalone package for your platform, you can use
`packaging/standalone/Makefile` as described at the beginning of this
section.

### Building all packages in one go

If you want to build all packages in one command, you can use the
following helpers:
```
# Automatically creates the source archive.
make packages

# Use an existing archive.
make -C packaging package SOURCE_DIST_FILE=...
```

However, be careful with the versioning! Because all package have
incompatible requirements, you can only use a version with 3 integers
(like a final semver-based version):
```
make packages PROJECT_VERSION=3.8.1
make -C packaging packages SOURCE_DIST_FILE=rabbitmq-server-3.8.1.tar.xz
```

If you do not follow that rule, the build will fail one way or another;
probably in the Windows package because of the *product version*
restrictions.

Another possibility is to specify the Windows *product version* and
rely on automatic conversion for Debian and RPM packages (or use the
`DEBIAN_VERSION` and `RPM_VERSION` variables), but this is untested:
```
make packages PROJECT_VERSION=3.8.1-rc.1 PRODUCT_VERSION=3.8.1.0
```
