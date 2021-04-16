#!/bin/sh
# vim:sw=2:et:

set -e

usage() {
  echo "Syntax: $(basename "$0") [-h] <uuid>"
}

while getopts "h" opt; do
  case $opt in
    h)
      usage
      exit
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      usage 1>&2
      exit 64
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      usage 1>&2
      exit 64
      ;;
  esac
done
shift $((OPTIND - 1))

uuid=$1
if test -z "$uuid"; then
  echo "Unique ID is required" 1>&2
  echo 1>&2
  usage
  exit 64
fi
shift

terraform_dir=$(cd "$(dirname "$0")" && pwd)

init_terraform() {
  terraform -chdir="$terraform_dir" init
}

query_vms() {
  terraform -chdir "$terraform_dir" apply \
    -auto-approve=true \
    -var="uuid=$uuid" \
    -var="erlang_nodename=control"
}

init_terraform

query_vms
