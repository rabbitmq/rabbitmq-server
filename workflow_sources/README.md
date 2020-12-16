# Overview

These are files used to build GitHub Actions workflows.

## Build

To generate the full workflow files in the `.github/` subdirectory:
```
# Change to base dir of the rabbitmq/rabbitmq-server clone
cd ..
make monorepo-actions
```

## Customization

Sometimes when diagnosing a failed test suite, you only wish to run tests for that suite in GitHub Actions. To do so, follow these steps:

* Check out a new branch if you haven't already.
* Remove everything but the suites you wish to run from `worflow_sources/deps.yml`. For instance, the following will only run `deps/rabbit/test/cluster_rename_SUITE.erl`:
```
#@data/values
---
#@overlay/match missing_ok=True
deps:
- name: rabbit
  test_suites_in_parallel: true
  suites:
  - name: cluster_rename
    time: 284
```
* Re-generate the workflow definitions:
```
make monorepo-actions
```
* Commit and push the changes.
