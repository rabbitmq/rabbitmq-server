# Automated End-to-End testing of the management ui with Selenium

We are using Selenium webdriver to simulate running the management ui in a browser.
And Mocha as the testing framework for Javascript.

To run the tests we need:
- make
- docker

# How tests are organized

All test cases and their configuration files are under the `test` folder and grouped into subfolders based on the area of functionality they are testing. For instance, under `oauth` folder, we have test cases about OAuth 2.0.
Furthermore, within an area of functionality like OAuth 2.0 we want to test under different configuration.
For instance under `test/oauth/with-uaa` we group all test cases which run against UAA. Whereas
under `test/oauth/with-uaa-down` we group all test cases which run against a UAA which is down.

And under `suites` folder we have the test suites where we literally script the following:
  - the suite's **setup**, e.g. start RabbitMQ and UAA with a specific configuration
  - the path to the test cases
  - the path to the test case configuration
  - the suite's **teardown**, e.g. stop RabbitMQ and UAA
  - and save all logs and screen captures if any

The idea is that every folder is a suite of test cases which have in common a runtime environment. For instance,
the test suite `test/oauth/with-uaa` has a `setup.sh` script which deploys RabbitMQ and a UAA server.
Whereas the test suite `test/oath/with-uaa-down` has a `setup.sh` script which only deploys RabbitMQ.

# How run the tests

But regardless how we run the tests, we are going to compile RabbitMQ server and build its docker image first.
```
make package-generic-unix
make docker-image
```
From the output of the `make docker-image` we copy the image's tag and run the following
command, e.g.
```
export RABBITMQ_IMAGE_TAG=3.8.10-1696.g171734f.dirty
```

There are two ways to run the tests.

**The interactive mode** - If we are writing code in any of RabbitMQ plugins or
libraries, we most likely want to run tests interactively, i.e. against our local installed Chrome browser, so that we
can see the simulated user interactions in the browser. In this mode, we are also running RabbitMQ directly
from source to speed things up. Otherwise, we would have to build a docker image for every change we wanted to test.

**Headless mode** - If we are not making any code changes to RabbitMQ and instead
we are only writing tests or simply we want to run them, then we want to run the tests in headless mode.


## Run tests in headless-mode

In this mode, we run suite of tests. This is how to run one suite:
```
make setup SUITE=test/oauth/with-uaa-down
make run-test SUITE=test/oauth/with-uaa-down
make teardown SUITE=test/oauth/with-uaa-down
```

**Note**: If at any stage, the tests take a long time to run, try restarting the selenium-hub by running. Sometimes it becomes unresponsive.
```
make start-chrome
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

Access the test suite folder:
```
cd test/oauth/with-uaa
```

Start UAA:
```
make start-uaa
```

Start RabbitMQ from source (it runs `make run-broker`):
```
make start-rabbitmq
```

<<<<<<< HEAD
To run all tests under the suite:
```
make run-test
=======
To run all tests under `with-uaa`:
```
make test TEST=with-uaa
```
Or to run a single tests under the suite:
```
make test TEST=with-uaa/landing.js
>>>>>>> 9354397cbf (Support Idp initiated logon in mgt ui with Oauth)
```

**VERY IMPORTANT NOTE**: `make start-rabbitmq` will always load `rabbitmq-localhost.config`
regardless of the test suite we are running. Therefore, if your suite requires a specific
configuration ensure that configuration is in `rabbitmq-localhost.config`.

If you had a specific configuration file, such as `rabbitmq-localhost-keycloak.config` you can run
`make start-rabbitmq` with that configuration like this:
```
<<<<<<< HEAD
make run-test TEST=landing.js
=======
make RABBITMQ_CONFIG_FILE=rabbitmq-localhost-keycloak.config start-rabbitmq
>>>>>>> 9354397cbf (Support Idp initiated logon in mgt ui with Oauth)
```

We do not have this issue when we run the headless suites because they use dedicated files
for each suite. Doing the same when running locally, i.e using `localhost`, would be too tedious.
