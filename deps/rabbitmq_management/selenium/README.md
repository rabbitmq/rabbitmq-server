# Automated End-to-End testing of the management ui with Selenium

We are using Selenium webdriver to simulate running the management ui in a browser.
And Mocha as the testing framework for Javascript.

To run the tests we need:
- make
- docker

# How tests are organized

All test cases and their configuration files are under the `test` folder and grouped into subfolders based on the area of functionality they are testing. For instance, under `oauth` folder, we have test cases about OAuth 2.0.
Furthermore, within an area of functionality like OAuth 2.0 we want to test under different configuration.
For instance under `test/oauth/with-sp-initiated` we group all test cases which run against an Authorization server via the Authorization Code Flow. Whereas under `test/oauth/with-idp-down` we group all test cases which run against an Authorization server which is down.

And under `suites` folder we have the test suites. A test suite is a script which executes all test cases
under a folder and using a given configuration and launching a number of components the test suite depends on.

Some test cases only depend on RabbitMQ such as the `basic-auth.sh`. In this test suite, we specify the location of the test case relative to the `test` folder.
And we call the function `run`. We always have to source the file under `bin/suite_template`.

```
TEST_CASES_PATH=/basic-auth

source $SCRIPT/../bin/suite_template
run
````

When our test suite requires of other components such as UAA, we use the function call `runWith` as we see below for the test case `oauth-with-uaa.sh`. In this test case, in addition to declaring the location of the test case, we also specify the location of the configuration files required to launch the components this suite
depends on. We also specify which profiles are activated which in turn defines the settings and configuration files used in the suite. And said earlier, the suite runs via the function `runWith` specifying the components that should be started and stopped and its logs captured too.

```
TEST_CASES_PATH=/oauth/with-sp-initiated
TEST_CONFIG_PATH=/oauth
PROFILES="uaa uaa-oauth-provider"

source $SCRIPT/../bin/suite_template
runWith uaa

```


# How to run the tests

There are two ways to run the tests.

**The interactive mode** - If we are writing code in any of RabbitMQ plugins or
libraries, we most likely want to run tests interactively, i.e. against our local installed Chrome browser, so that we
can see the simulated user interactions in the browser. In this mode, we are also running RabbitMQ directly
from source to speed things up. Otherwise, we would have to build a docker image for every change we wanted to test.

**Headless mode** - If we are not making any code changes to RabbitMQ and instead
we are only writing tests or simply we want to run them, then we want to run the tests in headless mode.

**IMPORTANT**: RabbitMQ and UAA configuration is different for each mode. The reason is the hostname used
for both, RabbitMQ and UAA. In headless mode, everything runs in containers and we refer to each container but its
name, e.g. `rabbitmq` or `uaa`. Whereas in interactive mode, we run most of the components locally (i.e `localhost`), such as RabbitMQ or UAA.

## Run tests in headless-mode

In this mode, we run suite of tests. This is how to run one suite:
```
suites/oauth-with-uaa.sh
```

And this is how we run all suites:
```
run-suites.sh
```

If you want to test your local changes, you can still build an image with these 2 commands from the
root folder of the `rabbitmq-server` repo:
```
cd ../../../../
make package-generic-unix
make docker-image
```

The last command prints something like this:
```
 => => naming to docker.io/pivotalrabbitmq/rabbitmq:3.11.0-rc.2.51.g4f3e539.dirty                                                                            0.0s
```

To run a suite with a particular docker image we do it like this:
```
cd deps/rabbitmq_management/selenium/suites
RABBITMQ_DOCKER_IMAGE=pivotalrabbitmq/rabbitmq:3.11.0-rc.2.51.g4f3e539.dirty ./oauth-with-uaa-with-mgt-prefix.sh
```

## Run tests interactively using your local chrome browser

In this mode when we are actively developing and hence we need full control of the
runtime environment for the tests.
For this reason, every test suite should have its own Makefile to help developer bootstrap
the required runtime.

Let's say we want to run the test suite `test/oauth/with-uaa`. We proceed as follows:

Get node.js dependencies ready. Selenium tests are ultimately written in Javascript and
requires node.js runtime to run them:
```
cd selenium
npm install
```

Start UAA:
```
cd test/oauth
make start-uaa
```

Start RabbitMQ from source (it runs `make run-broker`):
```
make start-rabbitmq
```

To run all tests under `with-uaa`:
```
make test TEST=with-uaa
```
Or to run a single tests under the suite:
```
make test TEST=with-uaa/landing.js
```

**VERY IMPORTANT NOTE**: `make start-rabbitmq` will always load `rabbitmq-localhost.config`
regardless of the test suite we are running. Therefore, if your suite requires a specific
configuration ensure that configuration is in `rabbitmq-localhost.config`.

If you had a specific configuration file, such as `rabbitmq-localhost-keycloak.config` you can run
`make start-rabbitmq` with that configuration like this:
```
make RABBITMQ_CONFIG_FILE=rabbitmq-localhost-keycloak.config start-rabbitmq
```

We do not have this issue when we run the headless suites because they use dedicated files
for each suite. Doing the same when running locally, i.e using `localhost`, would be too tedious.

## Chrome vs Chrome driver version mismatch

If you find the following error when you first attempt to run one of the selenium tests
```
SessionNotCreatedError: session not created: This version of ChromeDriver only supports Chrome version 108
Current browser version is 110.0.5481.100 with binary path /Applications/Google Chrome.app/Contents/MacOS/Google Chrome
```
It is because your current Chrome version is newer than the `chromedriver` configured in package.json.
```
  ....
  "dependencies": {
    "chromedriver": "^110.0.0",
  ...
```
To fix the problem, bump the version in your package.json to match your local chrome version and run again the
following command:
```
  npm install
```
