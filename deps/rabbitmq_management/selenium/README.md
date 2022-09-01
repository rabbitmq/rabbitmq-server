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

In this mode, we are running the following containers within the same
docker network in our local machine:
 - RabbitMQ
 - UAA
 - the browser  
 - and the test runner

First, we launch **Selenium Hub** which is where the headless chrome browser runs:
```
cd selenium
make run-chrome
```

Then we run the setup.sh script which deploys the runtime requirement for the suite test we are going to run:
```
make run-setup SUITE=test/oauth/with-uaa
```

And now we run the tests
```
make run-test-suite SUITE=test/oauth/with-uaa
```
> By default, if we do not specify any SUITE, it uses `test/oauth/with-uaa`

And to tear down the runtime for the test suite we run:
```
make run-teardown SUITE=test/oauth/with-uaa
```

At the moment there are only 2 suites of tests. The first one is `test/oauth/with-uaa` that we just discussed above.
The second one is `test/oauth/with-uaa-down` which runs a test case with UAA down.
To run this suite, we would proceed as follows:

```
make run-setup SUITE=test/oauth/with-uaa-down
make run-test-suite SUITE=test/oauth/with-uaa-down
make run-teardown SUITE=test/oauth/with-uaa-down
```

**Note**: If at any stage, the tests take a long time to run, try restarting the selenium-hub by running. Sometimes it becomes unresponsive.
```
make run-chrome
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

To run all tests under the suite:
```
make run-test
```
Or to run a single tests under the suite:
```
make run-test TEST=landing.js
```
