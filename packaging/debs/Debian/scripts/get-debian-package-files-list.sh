#!/bin/sh

set -e

changes_file=$1
if [ -z "$changes_file" ]; then
	echo "Syntax: $(basename $0) <changes file>" 1>&2
	exit 64
fi

if [ ! -f "$changes_file" ]; then
	exit 0
fi

changes_file_dir=$(dirname "$changes_file")
if test "$changes_file_dir" = '.'; then
	changes_file_dir=
else
	changes_file_dir="$changes_file_dir/"
fi

# Include the .changes file itself in the list.
echo "$changes_file_dir$(basename "$changes_file")"

# We want to match lines with the following format (it starts
# with a single space):
#  f752d307528f0ca87d57995c217344ec 5184732 net extra rabbitmq-(...)
awk '
/^ [a-fA-F0-9]+ / {
	if (length($1) == 32) {
		print "'$changes_file_dir'" $5;
	}
}' < "$changes_file"
