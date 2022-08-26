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

# Run tests interactively using your local chrome browser

Get node.js dependencies ready:
```
npm install
```

Get UAA and RabbitMQ Ready and wait until UAA is ready:
```
make local-setup
make wait-for-uaa
```

At this stage, we can run all the tests under `test/oauth/with-uaa` by running
```
make run-local-test TEST_ARGS=test/oauth/with-uaa
```

Or to test an individual test :
```
make run-local-test TEST_ARGS=test/oauth/with-uaa/landing.js
```
> By default, if we do not specify any TEST_ARGS, it uses `test/oauth/with-uaa`

# Run tests in headless-mode locally

Launch **Selenium Hub** which is where the headless chrome browser runs:
```
make run-chrome
```

Get UAA and RabbitMQ Ready and wait until UAA is ready:
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
