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

# How to run tests locally

First of all, we build RabbitMQ server and its docker image:
```
make package-generic-unix
make docker-image
```
From the output of the `make docker-image` we copy the image's tag and run the following
command:
```
export RABBITMQ_IMAGE_TAG=3.8.10-1696.g171734f.dirty
```
> The script bin/rabbitmq.sh uses this environment variable to launch RabbitMQ in a docker container

Once we have RabbitMQ compiled and its docker image ready we can run the tests.

## Run tests interactively using your local chrome browser

This is the best mode when we are still writing the tests and we want to see the
browser interactions.

Get node.js dependencies ready:
```
npm install
```

Run RabbitMQ from source (`it runs make run-broker`):
```
make local-rabbitmq
```

Run UAA, via its docker image, and wait until it is ready:
```
make local-uaa
make wait-for-uaa
```

At this stage, we can run all the tests under `test/oauth/with-uaa` by running
```
make run-local-test TEST_ARGS=test/oauth/with-uaa
```
> If it failed with the error `sh: mocha: command not found` you forgot to run `npm install`

Or to test an individual test :
```
make run-local-test TEST_ARGS=test/oauth/with-uaa/landing.js
```
> By default, if we do not specify any TEST_ARGS, it uses `test/oauth/with-uaa`

## Run tests in headless-mode locally

In this mode, both RabbitMQ and UAA and the browser and the test runner runs in docker containers.
That is why we need to build the latest RabbitMQ docker image.

First, we launch **Selenium Hub** which is where the headless chrome browser runs:
```
make run-chrome
```

Make sure you stopped RabbitMQ if you run it via `make local-rabbitmq`.

Then we start UAA and RabbitMQ and wait until UAA is ready:
```
make remote-setup
make wait-for-uaa
```

The difference between `remote-setup` and `local-setup` are the URLs each component
uses to refer to the other. For instance, we use `http://rabbitmq:15672` to access RabbitMQ UI
when we are running in `remote` mode because the browser is actually running in a container.

To run all tests under `test/oauth/with-uaa` invoke the following command:
```
make run-remote-test TEST_ARGS=test/oauth/with-uaa
```
> By default, if we do not specify any TEST_ARGS, it uses `test/oauth/with-uaa`


At the moment there are only 2 groups of tests. The first one is `oauth/with-uaa` that we just discussed above.
The second one is `oauth/with-uaa-down` which runs a test case with UAA down.
To run this suite, we need to shutdown UAA.

```
make stop-uaa
make run-remote-test TEST_ARGS=test/oauth/with-uaa-down
```
