# Automated End-to-End testing of the management ui with Selenium

We are using Selenium webdriver to simulate running the management ui in a browser.
And Mocha as the testing framework for Javascript.

To run the tests we need:
- make
- docker
- Ruby (needed to install `uaac` via `gem`)

# How tests are organized

All tests are hanging from the `test` folder. We can use subfolders to better organize them.
For instance, all OAuth2 related tests are under `test/oauth`. And under this folder
we have another subfolder, `tests/oauth/with-uaa` to group all the tests cases which run against UAA as OAuth2 server.

At the moment, there are no smart around discovering all the tests under subfolders. That will come later.
For now, the command `make run-tests` explicitly runs the test cases under `oauth/with-uaa`.

# Get environment ready before running any tests

Before running a test we need to set up its environment by running the command below. It will
launch RabbitMQ and UAA as a docker containers, and set up UAA with the right users, clients and scopes.  
```bash
test/oauth/with-uaa/setup.sh
```

**IMPORTANT**: When we run UAA and RabbitMQ in docker and the browser in our local machine, we have to use
the hostnames aliases `local-rabbitmq` and `local-uaa` which must be declared in our /etc/hosts file.
```
127.0.0.1 local-uaa local-rabbitmq
```


# Run all tests (WIP)

There are two ways to run the tests:
- **Developer way** - We run the tests against your locally installed
Chrome browser. This way we can see all the interactions driven by the tests on the browser and
should anything failed, we can see where it failed. To use this mode, we run the tests with the command `RUN_LOCAL=TRUE make run-tests`
- **Continuous integration way** - We run the tests against a standalone chrome browser which runs in silent mode (no UI) in
a docker container. To launch the standalone chrome browser we run `make run-chrome` followed by `make run-tests` to run the tests.

Both methods run the tests from a docker container using a docker image that we have to build first by running the following command:
```
make init-tests
```

In summary, if we want to run all tests against our local browser, we have to run these commands:
```
test/oauth/with-uaa/setup.sh
make init-tests
RUN_LOCAL=TRUE make run-tests

```

# Run single test

If we want to run a single tests rather than all tests under `oauth/with-uaa`, we run a command similar to this one:
```bash
RUN_LOCAL=TRUE ./node_modules/.bin/mocha  --timeout 20000 test/oauth/with-uaa/happy-login.js
```

## Run RabbitMQ from source

If we prefer, we can run RabbitMQ from source rather from a docker container, we launch it as follows:
```bash
gmake run-broker PLUGINS="rabbitmq_management rabbitmq_auth_backend_oauth2" RABBITMQ_CONFIG_FILE=deps/rabbitmq_management/selenium/test/oauth/with-uaa/rabbitmq.config
```
> Run the command from the root of rabbitmq-server checked out folder
