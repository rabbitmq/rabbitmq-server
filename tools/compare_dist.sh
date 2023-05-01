#!/usr/bin/env bash
set -euo pipefail

GOLDEN=$1
SECOND=$2

echo "Check both have INSTALL"
test -f $GOLDEN/rabbitmq_server-${VERSION}/INSTALL
test -f $SECOND/rabbitmq_server-${VERSION}/INSTALL

echo "Check LICENSEs"
diff \
    <(grep LICENSE make.manifest) \
    <(grep LICENSE bazel.manifest | grep -v ".md" | grep -v ".txt")

echo "Check plugins"
plugins_rel=rabbitmq_server-${VERSION}/plugins
diff <(grep $plugins_rel make.manifest | grep -v ".ez") <(grep $plugins_rel bazel.manifest | grep -v ".ez")

echo "Plugins exist with same version and deps"
for p in ${PLUGINS}; do
    echo "$p"
    f="$(cd $GOLDEN && ls -d $plugins_rel/$p-*)"
    test -f $GOLDEN/$f/ebin/$p.app || (echo "$GOLDEN/$f/ebin/$p.app does not exist"; exit 1)
    test -d $SECOND/$f || (echo "$SECOND/$f does not exist"; exit 1)
    test -f $SECOND/$f/ebin/$p.app || (echo "$SECOND/$f/ebin/$p.app does not exist"; exit 1)
    ./rabbitmq-server/tools/erlang_app_equal \
    $GOLDEN/$f/ebin/$p.app \
    $SECOND/$f/ebin/$p.app
done

echo "Both have escript"
escript_rel=rabbitmq_server-${VERSION}/escript
diff <(grep $escript_rel make.manifest) <(grep $escript_rel bazel.manifest)

echo "Both have sbin"
sbin_rel=rabbitmq_server-${VERSION}/sbin
diff <(grep $sbin_rel make.manifest) <(grep $sbin_rel bazel.manifest)

echo "Both have manpages"
manpages_rel=rabbitmq_server-${VERSION}/share/man
diff <(grep $manpages_rel make.manifest) <(grep $manpages_rel bazel.manifest)

echo "PASS"
