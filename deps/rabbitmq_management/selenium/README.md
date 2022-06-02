# Automated End-to-End testing of the management ui with Selenium

We are using Selenium webdriver to simulate running the management ui in a browser.
And Mocha as the testing framework for Javascript.

# Run the tests

To run the tests first we run the following command to launch Selenium for Chrome.
```
make run-chrome
```

And then to run the actual tests we run:
```
make run-tests
```

# How tests are organized

All tests are hanging from the `tests` folder. We can use subfolders to better organize them.
For instance, all Oauth2 related tests are under `tests/oauth`. And under this folder
we have another subfolder, `tests/oauth/with-uaa` to group all the tests cases which run against UAA as Oauth2 server.

At the moment, there are no smart around discovering all the tests under subfolders. That will come later.
For now, the command `make run-tests` runs just the test cases under `oauth/with-uaa`. But first, the script `setup.sh` is
invoked which initializes the fixtures for the tests under `oauth/with-uaa`. More specifically, it launches rabbitmq
configured with UAA, and also UAA configured with a number of users, permissions, and clients.
