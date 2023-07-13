#!/usr/bin/env bash
set -uo pipefail

GOLDEN=$1
SECOND=$2

failure_count=0

echo "Check both have INSTALL"
test -f $GOLDEN/rabbitmq_server-${VERSION}/INSTALL || ((failure_count++))
test -f $SECOND/rabbitmq_server-${VERSION}/INSTALL || ((failure_count++))

echo "Check LICENSEs"
diff \
    <(grep LICENSE make.manifest) \
    <(grep LICENSE bazel.manifest | grep -v ".md" | grep -v ".txt") \
         || ((failure_count++))

echo "Check plugins"
plugins_rel=rabbitmq_server-${VERSION}/plugins
diff \
    <(grep $plugins_rel make.manifest | grep -v ".ez") \
    <(grep $plugins_rel bazel.manifest | grep -v ".ez") \
         || ((failure_count++))

echo "Plugins exist with same version and deps"
for p in ${PLUGINS} ${EXTRA_PLUGINS}; do
    echo "$p"
    f="$(cd $GOLDEN && ls -d $plugins_rel/$p-*)"
    test -f $GOLDEN/$f/ebin/$p.app || (echo "$GOLDEN/$f/ebin/$p.app does not exist"; ((failure_count++)))
    test -d $SECOND/$f || (echo "$SECOND/$f does not exist"; ((failure_count++)))
    test -f $SECOND/$f/ebin/$p.app || (echo "$SECOND/$f/ebin/$p.app does not exist"; ((failure_count++)))
    ./rabbitmq-server/tools/erlang_app_equal \
        $GOLDEN/$f/ebin/$p.app \
        $SECOND/$f/ebin/$p.app \
             || ((failure_count++))
done

echo "Both have escript"
escript_rel=rabbitmq_server-${VERSION}/escript
diff \
    <(grep $escript_rel make.manifest) \
    <(grep $escript_rel bazel.manifest) \
         || ((failure_count++))

echo "Both have sbin"
sbin_rel=rabbitmq_server-${VERSION}/sbin
diff \
    <(grep $sbin_rel make.manifest) \
    <(grep $sbin_rel bazel.manifest) \
         || ((failure_count++))

echo "Both have manpages"
manpages_rel=rabbitmq_server-${VERSION}/share/man
diff \
    <(grep $manpages_rel make.manifest) \
    <(grep $manpages_rel bazel.manifest) \
         || ((failure_count++))

echo "There were $failure_count failures."

exit $failure_count
