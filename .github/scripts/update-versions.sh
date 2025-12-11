#!/bin/bash

# Updates RabbitMQ and Erlang versions in discussion template
# Fetches latest versions from GitHub releases and Erlang OTP versions table

set -o errexit
set -o pipefail
# set -o xtrace

if [[ -d $GITHUB_WORKSPACE ]]
then
    repo_root="$GITHUB_WORKSPACE"
else
    repo_root="$(CDPATH='' cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
declare -r repo_root

set -o nounset

declare -r versions_file="$repo_root/.github/versions.json"
declare -r questions_yml_file="$repo_root/.github/DISCUSSION_TEMPLATE/questions.yml"

fetch_rabbitmq_versions() {
    local -a all_versions
    local version

    while IFS= read -r version
    do
        all_versions+=("$version")
    done < <(gh release list \
        --repo rabbitmq/rabbitmq-server \
        --limit 1000 \
        --exclude-drafts \
        --exclude-pre-releases \
        --json tagName \
        --jq '.[].tagName | select(startswith("v4.")) | ltrimstr("v")')

    if (( ${#all_versions[@]} == 0 ))
    then
        echo "Error: No RabbitMQ versions found" >&2
        exit 1
    fi

    local -A seen_minors
    local -a filtered_versions
    local -i minor_count=0

    for version in "${all_versions[@]}"
    do
        local minor
        minor="${version%.*}"

        if [[ -z "${seen_minors[$minor]:-}" ]]
        then
            seen_minors["$minor"]=1
            ((++minor_count))

            if (( minor_count > 2 ))
            then
                break
            fi
        fi

        filtered_versions+=("$version")
    done

    printf '%s\n' "${filtered_versions[@]}"
}

fetch_erlang_versions() {
    local -a all_versions
    local -a unique_minors
    local version

    while IFS= read -r line
    do
        if [[ "$line" =~ ^OTP-([0-9]+\.[0-9]+) ]]
        then
            local major
            major="${BASH_REMATCH[1]%%.*}"

            if (( major < 26 ))
            then
                break
            fi

            version="${BASH_REMATCH[1]}.x"
            all_versions+=("$version")
        fi
    done < <(curl -fsSL https://raw.githubusercontent.com/erlang/otp/refs/heads/master/otp_versions.table 2>/dev/null)

    if (( ${#all_versions[@]} == 0 ))
    then
        echo "Error: No Erlang versions found" >&2
        exit 1
    fi

    local -A seen_minors
    for version in "${all_versions[@]}"
    do
        if [[ -z "${seen_minors[$version]:-}" ]]
        then
            seen_minors["$version"]=1
            unique_minors+=("$version")
        fi
    done

    local -a sorted_versions
    while IFS= read -r version
    do
        sorted_versions+=("$version")
    done < <(printf '%s\n' "${unique_minors[@]}" | sort -V -r)

    local -i major_count=0
    local current_major=""
    local -a filtered_versions

    for version in "${sorted_versions[@]}"
    do
        local major
        major="${version%%.*}"

        if [[ "$major" != "$current_major" ]]
        then
            current_major="$major"
            ((++major_count))

            if (( major_count > 3 ))
            then
                break
            fi
        fi

        filtered_versions+=("$version")
    done

    printf '%s\n' "${filtered_versions[@]}"
}

update_versions_json() {
    local -a rabbitmq_versions
    local -a erlang_versions

    while IFS= read -r version
    do
        rabbitmq_versions+=("$version")
    done < <(fetch_rabbitmq_versions)

    while IFS= read -r version
    do
        erlang_versions+=("$version")
    done < <(fetch_erlang_versions)

    local rabbitmq_json
    rabbitmq_json=$(printf '%s\n' "${rabbitmq_versions[@]}" | jq -R . | jq -s .)

    local erlang_json
    erlang_json=$(printf '%s\n' "${erlang_versions[@]}" | jq -R . | jq -s .)

    jq -n \
        --argjson rabbitmq "$rabbitmq_json" \
        --argjson erlang "$erlang_json" \
        '{rabbitmq: $rabbitmq, erlang: $erlang}' > "$versions_file"
}

update_template_file() {
    local versions_yaml_file
    versions_yaml_file="$(mktemp)"

    jq -r 'to_entries | map("\(.key):\n" + (.value | map("- \(.)") | join("\n"))) | join("\n")' "$versions_file" > "$versions_yaml_file"

    ytt --file - --data-values-file "$versions_yaml_file" \
        < "$repo_root/.github/DISCUSSION_TEMPLATE/questions.yml.tpl" \
        > "$questions_yml_file"

    rm "$versions_yaml_file"
}

check_for_changes() {
    if git diff --quiet "$versions_file" "$questions_yml_file"
    then
        echo "No version changes detected"
        exit 0
    fi

    echo "Version changes detected"
}

main() {
    echo "Fetching latest versions..."
    update_versions_json

    echo "Updating discussion template..."
    update_template_file

    check_for_changes
}

main
