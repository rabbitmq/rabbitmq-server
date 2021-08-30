#!/bin/sh
# vim:sw=4:et:

set -ex

slapd_data_dir=$1
tcp_port=$2

pidfile="$slapd_data_dir/slapd.pid"
uri="ldap://localhost:$tcp_port"

binddn="cn=config"
passwd=secret

case "$(uname -s)" in
    Linux)
        slapd=/usr/sbin/slapd
        modulepath=/usr/lib/ldap
        schema_dir=/etc/ldap/schema
        ;;
    FreeBSD)
        slapd=/usr/local/libexec/slapd
        modulepath=/usr/local/libexec/openldap
        schema_dir=/usr/local/etc/openldap/schema
        ;;
    *)
        exit 1
        ;;
esac

# --------------------------------------------------------------------
# slapd(8) configuration + start
# --------------------------------------------------------------------

rm -rf "$slapd_data_dir"
mkdir -p "$slapd_data_dir"

conf_file=$slapd_data_dir/slapd.conf
cat <<EOF > "$conf_file"
include         $schema_dir/core.schema
include         $schema_dir/cosine.schema
include         $schema_dir/nis.schema
include         $schema_dir/inetorgperson.schema
pidfile         $pidfile
modulepath      $modulepath
loglevel        7

database        config
rootdn          "$binddn"
rootpw          $passwd
EOF

cat "$conf_file"

conf_dir=$slapd_data_dir/slapd.d
mkdir -p "$conf_dir"

# Start slapd(8).
"$slapd" \
    -f "$conf_file" \
    -F "$conf_dir" \
    -h "$uri"

auth="-x -D $binddn -w $passwd"

# We wait for the server to start.
for seconds in 1 2 3 4 5 6 7 8 9 10; do
    ldapsearch $auth -H "$uri" -LLL -b cn=config dn && break;
    sleep 1
done

# --------------------------------------------------------------------
# Load the example LDIFs for the testsuite.
# --------------------------------------------------------------------

script_dir=$(cd "$(dirname "$0")" && pwd)
example_ldif_dir="$script_dir/../../example"
example_data_dir="$slapd_data_dir/example"
mkdir -p "$example_data_dir"

# We update the hard-coded database directory with the one we computed
# here, so the data is located inside the test directory.
sed -E -e "s,^olcDbDirectory:.*,olcDbDirectory: $example_data_dir," \
    < "$example_ldif_dir/global.ldif" | \
    ldapadd $auth -H "$uri"

# We remove the module path from the example LDIF as it was already
# configured.
sed -E -e "s,^olcModulePath:.*,olcModulePath: $modulepath," \
    < "$example_ldif_dir/memberof_init.ldif" | \
    ldapadd $auth -H "$uri"

ldapmodify $auth -H "$uri" -f "$example_ldif_dir/refint_1.ldif"
ldapadd $auth -H "$uri" -f "$example_ldif_dir/refint_2.ldif"

ldapsearch $auth -H "$uri" -LLL -b cn=config dn

echo SLAPD_PID=$(cat "$pidfile")
