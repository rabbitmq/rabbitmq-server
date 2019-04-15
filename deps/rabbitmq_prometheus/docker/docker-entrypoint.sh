#!/bin/bash
set -eu

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# backwards compatibility for old environment variables
: "${RABBITMQ_SSL_CERTFILE:=${RABBITMQ_SSL_CERT_FILE:-}}"
: "${RABBITMQ_SSL_KEYFILE:=${RABBITMQ_SSL_KEY_FILE:-}}"
: "${RABBITMQ_SSL_CACERTFILE:=${RABBITMQ_SSL_CA_FILE:-}}"

# "management" SSL config should default to using the same certs
: "${RABBITMQ_MANAGEMENT_SSL_CACERTFILE:=$RABBITMQ_SSL_CACERTFILE}"
: "${RABBITMQ_MANAGEMENT_SSL_CERTFILE:=$RABBITMQ_SSL_CERTFILE}"
: "${RABBITMQ_MANAGEMENT_SSL_KEYFILE:=$RABBITMQ_SSL_KEYFILE}"

# Allowed env vars that will be read from mounted files (i.e. Docker Secrets):
fileEnvKeys=(
	default_user
	default_pass
)

# https://www.rabbitmq.com/configure.html
sslConfigKeys=(
	cacertfile
	certfile
	depth
	fail_if_no_peer_cert
	keyfile
	verify
)
managementConfigKeys=(
	"${sslConfigKeys[@]/#/ssl_}"
)
rabbitConfigKeys=(
	default_pass
	default_user
	default_vhost
	hipe_compile
	vm_memory_high_watermark
)
fileConfigKeys=(
	management_ssl_cacertfile
	management_ssl_certfile
	management_ssl_keyfile
	ssl_cacertfile
	ssl_certfile
	ssl_keyfile
)
allConfigKeys=(
	"${managementConfigKeys[@]/#/management_}"
	"${rabbitConfigKeys[@]}"
	"${sslConfigKeys[@]/#/ssl_}"
)

declare -A configDefaults=(
	[management_ssl_fail_if_no_peer_cert]='false'
	[management_ssl_verify]='verify_none'

	[ssl_fail_if_no_peer_cert]='true'
	[ssl_verify]='verify_peer'
)

# allow the container to be started with `--user`
if [[ "$1" == rabbitmq* ]] && [ "$(id -u)" = '0' ]; then
	# this needs to happen late enough that we have the SSL config
	# https://github.com/docker-library/rabbitmq/issues/283
	for conf in "${allConfigKeys[@]}"; do
		var="RABBITMQ_${conf^^}"
		val="${!var:-}"
		[ -n "$val" ] || continue
		case "$conf" in
			*_ssl_*file | ssl_*file )
				if [ -f "$val" ] && ! gosu rabbitmq test -r "$val"; then
					newFile="/tmp/rabbitmq-ssl/$conf.pem"
					echo >&2
					echo >&2 "WARNING: '$val' ($var) is not readable by rabbitmq ($(id rabbitmq)); copying to '$newFile'"
					echo >&2
					cat "$val" > "$newFile"
					chown rabbitmq "$newFile"
					chmod 0400 "$newFile"
					eval 'export '$var'="$newFile"'
				fi
				;;
		esac
	done

	if [ "$1" = 'rabbitmq-server' ]; then
		find /var/lib/rabbitmq \! -user rabbitmq -exec chown rabbitmq '{}' +
	fi

	exec gosu rabbitmq "$BASH_SOURCE" "$@"
fi

haveConfig=
haveSslConfig=
haveManagementSslConfig=
for fileEnvKey in "${fileEnvKeys[@]}"; do file_env "RABBITMQ_${fileEnvKey^^}"; done
for conf in "${allConfigKeys[@]}"; do
	var="RABBITMQ_${conf^^}"
	val="${!var:-}"
	if [ "$val" ]; then
		if [ "${configDefaults[$conf]:-}" ] && [ "${configDefaults[$conf]}" = "$val" ]; then
			# if the value set is the same as the default, treat it as if it isn't set
			continue
		fi
		haveConfig=1
		case "$conf" in
			ssl_*) haveSslConfig=1 ;;
			management_ssl_*) haveManagementSslConfig=1 ;;
		esac
	fi
done
if [ "$haveSslConfig" ]; then
	missing=()
	for sslConf in cacertfile certfile keyfile; do
		var="RABBITMQ_SSL_${sslConf^^}"
		val="${!var}"
		if [ -z "$val" ]; then
			missing+=( "$var" )
		fi
	done
	if [ "${#missing[@]}" -gt 0 ]; then
		{
			echo
			echo 'error: SSL requested, but missing required configuration'
			for miss in "${missing[@]}"; do
				echo "  - $miss"
			done
			echo
		} >&2
		exit 1
	fi
fi
missingFiles=()
for conf in "${fileConfigKeys[@]}"; do
	var="RABBITMQ_${conf^^}"
	val="${!var}"
	if [ "$val" ] && [ ! -f "$val" ]; then
		missingFiles+=( "$val ($var)" )
	fi
done
if [ "${#missingFiles[@]}" -gt 0 ]; then
	{
		echo
		echo 'error: files specified, but missing'
		for miss in "${missingFiles[@]}"; do
			echo "  - $miss"
		done
		echo
	} >&2
	exit 1
fi

# set defaults for missing values (but only after we're done with all our checking so we don't throw any of that off)
for conf in "${!configDefaults[@]}"; do
	default="${configDefaults[$conf]}"
	var="RABBITMQ_${conf^^}"
	[ -z "${!var:-}" ] || continue
	eval "export $var=\"\$default\""
done

# if long and short hostnames are not the same, use long hostnames
if [ "$(hostname)" != "$(hostname -s)" ]; then
	: "${RABBITMQ_USE_LONGNAME:=true}"
fi

if [ "${RABBITMQ_ERLANG_COOKIE:-}" ]; then
	cookieFile='/var/lib/rabbitmq/.erlang.cookie'
	if [ -e "$cookieFile" ]; then
		if [ "$(cat "$cookieFile" 2>/dev/null)" != "$RABBITMQ_ERLANG_COOKIE" ]; then
			echo >&2
			echo >&2 "warning: $cookieFile contents do not match RABBITMQ_ERLANG_COOKIE"
			echo >&2
		fi
	else
		echo "$RABBITMQ_ERLANG_COOKIE" > "$cookieFile"
	fi
	chmod 600 "$cookieFile"
fi

configBase="${RABBITMQ_CONFIG_FILE:-/etc/rabbitmq/rabbitmq}"
oldConfigFile="$configBase.config"
newConfigFile="$configBase.conf"

shouldWriteConfig="$haveConfig"
if [ -n "$shouldWriteConfig" ] && [ -f "$oldConfigFile" ]; then
	{
		echo "error: Docker configuration environment variables specified, but old-style (Erlang syntax) configuration file '$oldConfigFile' exists"
		echo "  Suggested fixes: (choose one)"
		echo "   - remove '$oldConfigFile'"
		echo "   - remove any Docker-specific 'RABBITMQ_...' environment variables"
		echo "   - convert '$oldConfigFile' to the newer sysctl format ('$newConfigFile'); see https://www.rabbitmq.com/configure.html#config-file"
	} >&2
	exit 1
fi
if [ -z "$shouldWriteConfig" ] && [ ! -f "$oldConfigFile" ] && [ ! -f "$newConfigFile" ]; then
	# no config files, we should write one
	shouldWriteConfig=1
fi

# http://stackoverflow.com/a/2705678/433558
sed_escape_lhs() {
	echo "$@" | sed -e 's/[]\/$*.^|[]/\\&/g'
}
sed_escape_rhs() {
	echo "$@" | sed -e 's/[\/&]/\\&/g'
}
rabbit_set_config() {
	local key="$1"; shift
	local val="$1"; shift

	[ -e "$newConfigFile" ] || touch "$newConfigFile"

	local sedKey="$(sed_escape_lhs "$key")"
	local sedVal="$(sed_escape_rhs "$val")"
	sed -ri \
		"s/^[[:space:]]*(${sedKey}[[:space:]]*=[[:space:]]*)\S.*\$/\1${sedVal}/" \
		"$newConfigFile"
	if ! grep -qE "^${sedKey}[[:space:]]*=" "$newConfigFile"; then
		echo "$key = $val" >> "$newConfigFile"
	fi
}
rabbit_comment_config() {
	local key="$1"; shift

	[ -e "$newConfigFile" ] || touch "$newConfigFile"

	local sedKey="$(sed_escape_lhs "$key")"
	sed -ri \
		"s/^[[:space:]]*#?[[:space:]]*(${sedKey}[[:space:]]*=[[:space:]]*\S.*)\$/# \1/" \
		"$newConfigFile"
}
rabbit_env_config() {
	local prefix="$1"; shift

	local conf
	for conf; do
		local var="rabbitmq${prefix:+_$prefix}_$conf"
		var="${var^^}"

		local key="$conf"
		case "$prefix" in
			ssl) key="ssl_options.$key" ;;
			management_ssl) key="management.listener.ssl_opts.$key" ;;
		esac

		local val="${!var:-}"
		local rawVal="$val"
		case "$conf" in
			fail_if_no_peer_cert|hipe_compile)
				case "${val,,}" in
					false|no|0|'') rawVal='false' ;;
					true|yes|1|*) rawVal='true' ;;
				esac
				;;

			vm_memory_high_watermark) continue ;; # handled separately
		esac

		if [ -n "$rawVal" ]; then
			rabbit_set_config "$key" "$rawVal"
		else
			rabbit_comment_config "$key"
		fi
	done
}

if [ "$1" = 'rabbitmq-server' ] && [ "$shouldWriteConfig" ]; then
	rabbit_set_config 'loopback_users.guest' 'false'

	# determine whether to set "vm_memory_high_watermark" (based on cgroups)
	memTotalKb=
	if [ -r /proc/meminfo ]; then
		memTotalKb="$(awk -F ':? +' '$1 == "MemTotal" { print $2; exit }' /proc/meminfo)"
	fi
	memLimitB=
	if [ -r /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
		# "18446744073709551615" is a valid value for "memory.limit_in_bytes", which is too big for Bash math to handle
		# "$(( 18446744073709551615 / 1024 ))" = 0; "$(( 18446744073709551615 * 40 / 100 ))" = 0
		memLimitB="$(awk -v totKb="$memTotalKb" '{
			limB = $0;
			limKb = limB / 1024;
			if (!totKb || limKb < totKb) {
				printf "%.0f\n", limB;
			}
		}' /sys/fs/cgroup/memory/memory.limit_in_bytes)"
	fi
	if [ -n "$memLimitB" ]; then
		# if we have a cgroup memory limit, let's inform RabbitMQ of what it is (so it can calculate vm_memory_high_watermark properly)
		# https://github.com/rabbitmq/rabbitmq-server/pull/1234
		rabbit_set_config 'total_memory_available_override_value' "$memLimitB"
	fi
	# https://www.rabbitmq.com/memory.html#memsup-usage
	if [ "${RABBITMQ_VM_MEMORY_HIGH_WATERMARK:-}" ]; then
		# https://github.com/docker-library/rabbitmq/pull/105#issuecomment-242165822
		vmMemoryHighWatermark="$(
			echo "$RABBITMQ_VM_MEMORY_HIGH_WATERMARK" | awk '
				/^[0-9]*[.][0-9]+$|^[0-9]+([.][0-9]+)?%$/ {
					perc = $0;
					if (perc ~ /%$/) {
						gsub(/%$/, "", perc);
						perc = perc / 100;
					}
					if (perc > 1.0 || perc < 0.0) {
						printf "error: invalid percentage for vm_memory_high_watermark: %s (must be >= 0%%, <= 100%%)\n", $0 > "/dev/stderr";
						exit 1;
					}
					printf "vm_memory_high_watermark.relative %0.03f\n", perc;
					next;
				}
				/^[0-9]+$/ {
					printf "vm_memory_high_watermark.absolute %s\n", $0;
					next;
				}
				/^[0-9]+([.][0-9]+)?[a-zA-Z]+$/ {
					printf "vm_memory_high_watermark.absolute %s\n", $0;
					next;
				}
				{
					printf "error: unexpected input for vm_memory_high_watermark: %s\n", $0;
					exit 1;
				}
			'
		)"
		if [ "$vmMemoryHighWatermark" ]; then
			vmMemoryHighWatermarkKey="${vmMemoryHighWatermark%% *}"
			vmMemoryHighWatermarkVal="${vmMemoryHighWatermark#$vmMemoryHighWatermarkKey }"
			rabbit_set_config "$vmMemoryHighWatermarkKey" "$vmMemoryHighWatermarkVal"
			case "$vmMemoryHighWatermarkKey" in
				# make sure we only set one or the other
				'vm_memory_high_watermark.absolute') rabbit_comment_config 'vm_memory_high_watermark.relative' ;;
				'vm_memory_high_watermark.relative') rabbit_comment_config 'vm_memory_high_watermark.absolute' ;;
			esac
		fi
	fi

	if [ "$haveSslConfig" ]; then
		rabbit_set_config 'listeners.ssl.default' 5671
		rabbit_env_config 'ssl' "${sslConfigKeys[@]}"
	else
		rabbit_set_config 'listeners.tcp.default' 5672
	fi

	rabbit_env_config '' "${rabbitConfigKeys[@]}"

	# if management plugin is installed, generate config for it
	# https://www.rabbitmq.com/management.html#configuration
	if [ "$(rabbitmq-plugins list -q -m -e 'rabbitmq_management$')" ]; then
		if [ "$haveManagementSslConfig" ]; then
			rabbit_set_config 'management.listener.port' 15671
			rabbit_set_config 'management.listener.ssl' 'true'
			rabbit_env_config 'management_ssl' "${sslConfigKeys[@]}"
		else
			rabbit_set_config 'management.listener.port' 15672
			rabbit_set_config 'management.listener.ssl' 'false'
		fi

		# if definitions file exists, then load it
		# https://www.rabbitmq.com/management.html#load-definitions
		managementDefinitionsFile='/etc/rabbitmq/definitions.json'
		if [ -f "$managementDefinitionsFile" ]; then
			# see also https://github.com/docker-library/rabbitmq/pull/112#issuecomment-271485550
			rabbit_set_config 'management.load_definitions' "$managementDefinitionsFile"
		fi
	fi
fi

combinedSsl='/tmp/rabbitmq-ssl/combined.pem'
if [ "$haveSslConfig" ] && [[ "$1" == rabbitmq* ]] && [ ! -f "$combinedSsl" ]; then
	# Create combined cert
	cat "$RABBITMQ_SSL_CERTFILE" "$RABBITMQ_SSL_KEYFILE" > "$combinedSsl"
	chmod 0400 "$combinedSsl"
fi
if [ "$haveSslConfig" ] && [ -f "$combinedSsl" ]; then
	# More ENV vars for make clustering happiness
	# we don't handle clustering in this script, but these args should ensure
	# clustered SSL-enabled members will talk nicely
	export ERL_SSL_PATH="$(erl -eval 'io:format("~p", [code:lib_dir(ssl, ebin)]),halt().' -noshell)"
	sslErlArgs="-pa $ERL_SSL_PATH -proto_dist inet_tls -ssl_dist_opt server_certfile $combinedSsl -ssl_dist_opt server_secure_renegotiate true client_secure_renegotiate true"
	export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="${RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS:-} $sslErlArgs"
	export RABBITMQ_CTL_ERL_ARGS="${RABBITMQ_CTL_ERL_ARGS:-} $sslErlArgs"
fi

exec "$@"
