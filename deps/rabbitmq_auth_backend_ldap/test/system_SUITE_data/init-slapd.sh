#!/bin/sh
# vim:sw=4:et:

set -ex

readonly slapd_data_dir="$1"
readonly tcp_port="$2"

readonly pidfile="$slapd_data_dir/slapd.pid"
readonly uri="ldap://localhost:$tcp_port"

readonly binddn="cn=config"
readonly passwd=secret

case "$(uname -s)" in
    Linux)
        if [ -x /usr/bin/slapd ]
        then
            readonly slapd=/usr/bin/slapd
        elif [ -x /usr/sbin/slapd ]
        then
            readonly slapd=/usr/sbin/slapd
        fi

        if [ -d /usr/lib/openldap ]
        then
            readonly modulepath=/usr/lib/openldap
        elif [ -d /usr/lib/ldap ]
        then
            readonly modulepath=/usr/lib/ldap
        fi

        if [ -d /etc/openldap/schema ]
        then
            readonly schema_dir=/etc/openldap/schema
        elif [ -d /etc/ldap/schema ]
        then
            readonly schema_dir=/etc/ldap/schema
        fi
        ;;
    FreeBSD)
        readonly slapd=/usr/local/libexec/slapd
        readonly modulepath=/usr/local/libexec/openldap
        readonly schema_dir=/usr/local/etc/openldap/schema
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

readonly conf_file="$slapd_data_dir/slapd.conf"
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

readonly conf_dir="$slapd_data_dir/slapd.d"
mkdir -p "$conf_dir"

# Start slapd(8).
"$slapd" \
    -f "$conf_file" \
    -F "$conf_dir" \
    -h "$uri"

readonly auth="-x -D $binddn -w $passwd"

# We wait for the server to start.
# shellcheck disable=SC2034
for seconds in 1 2 3 4 5 6 7 8 9 10; do
    # shellcheck disable=SC2086
    ldapsearch $auth -H "$uri" -LLL -b cn=config dn && break;
    sleep 1
done

# --------------------------------------------------------------------
# Load the example LDIFs for the testsuite.
# --------------------------------------------------------------------

tmp="$(cd "$(dirname "$0")" && pwd)"
readonly script_dir="$tmp"
readonly example_ldif_dir="$script_dir/../../example"
readonly example_data_dir="$slapd_data_dir/example"
mkdir -p "$example_data_dir"

# We update the hard-coded database directory with the one we computed
# here, so the data is located inside the test directory.
# shellcheck disable=SC2086
sed -E -e "s,^olcDbDirectory:.*,olcDbDirectory: $example_data_dir," \
    < "$example_ldif_dir/global.ldif" | \
    ldapadd $auth -H "$uri"

# We remove the module path from the example LDIF as it was already
# configured.
# shellcheck disable=SC2086
sed -E -e "s,^olcModulePath:.*,olcModulePath: $modulepath," \
    < "$example_ldif_dir/memberof_init.ldif" | \
    ldapadd $auth -H "$uri"

# shellcheck disable=SC2086
ldapmodify $auth -H "$uri" -f "$example_ldif_dir/refint_1.ldif"

# shellcheck disable=SC2086
ldapadd $auth -H "$uri" -f "$example_ldif_dir/refint_2.ldif"

# shellcheck disable=SC2086
ldapsearch $auth -H "$uri" -LLL -b cn=config dn

echo SLAPD_PID="$(cat "$pidfile")"
