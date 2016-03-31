httpc-aws
=========
A thin wrapper around httpc for making requests to the Amazon Web Services API.

[![Build Status](https://travis-ci.org/gmr/httpc-aws.svg?branch=master)](https://travis-ci.org/gmr/httpc-aws)
[![codecov.io](https://codecov.io/github/gmr/httpc-aws/coverage.svg?branch=master)](https://codecov.io/github/gmr/httpc-aws?branch=master)

Environment Variables
---------------------
- ``AWS_DEFAULT_PROFILE``
- ``AWS_DEFAULT_REGION``
- ``AWS_CONFIG_FILE``
- ``AWS_SHARED_CREDENTIALS_FILE``
- ``AWS_ACCESS_KEY_ID``
- ``AWS_SECRET_ACCESS_KEY``

Configuration Precedence
------------------------
The configuration values have the following precedence:

- Request specific settings
- Environment variables
- Configuration file

Credentials Precedence
----------------------
The credentials values have the following precedence:

- Request specific settings
- Environment variables
- Credentials file


Build
-----

```bash
$ rebar3 compile
```

Test
----

```bash
$ rebar3 eunit
$ rebar3 cover
$ rebar3 dialyzer
```
