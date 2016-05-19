# httpc-aws

A light-weight, relatively un-opinionated Amazon Web Services API client for Erlang 17.5+.

[![Build Status](https://travis-ci.org/gmr/httpc-aws.svg?branch=master)](https://travis-ci.org/gmr/httpc-aws)
[![codecov.io](https://codecov.io/github/gmr/httpc-aws/coverage.svg?branch=master)](https://codecov.io/github/gmr/httpc-aws?branch=master)

## Supported Erlang Versions

 - 17.5
 - 18.0
 - 18.1
 - 18.2
 - 18.2.1
 - 18.3
 
## Configuration

Configuration for *httpc-aws* is can be provided in multiple ways. It is designed
to behave similarly to the [AWS Command Line Interface](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
with respect to providing region and configuratoin information. Additionally it
has two methods, ``httpc_aws:set_region/1`` and ``httpc_aws:set_credentials/2``
to allow for application specific configuration, bypassing the automatic configuration
behavior.

### Configuration Precedence

The configuration values have the following precedence:

 - Explicitly configured via API
 - Environment variables
 - Configuration file
 - EC2 Instance Metadata Service where applicable

### Credentials Precedence

The credentials values have the following precedence:

 - Explicitly configured via API
 - Environment variables
 - Credentials file
 - EC2 Instance Metadata Service
 
### Environment Variables

As with the AWS CLI, the following environment variables can be used to provide 
configuration or to impact configuration behavior:

 - ``AWS_DEFAULT_PROFILE``
 - ``AWS_DEFAULT_REGION``
 - ``AWS_CONFIG_FILE``
 - ``AWS_SHARED_CREDENTIALS_FILE``
 - ``AWS_ACCESS_KEY_ID``
 - ``AWS_SECRET_ACCESS_KEY``
 
## API Methods
 
  Method                             | Description
 ------------------------------------|--------------------------------------------------------------------------------------------
 ``httpc_aws:set_region/1``          | Manually specify the AWS region to make requests to.
 ``httpc_aws:set_credentials/2``     | Manually specify the request credentials to use.
 ``httpc_aws:refresh_credentials/0`` | Refresh the credentials from the environment, filesystem, or EC2 Instance Metadata service.
 ``httpc_aws:get/2``                 | Perform a GET request to the API specifying the service and request path.
 ``httpc_aws:get/3``                 | Perform a GET request specifying the service, path, and headers.
 ``httpc_aws:post/4``                | Perform a POST request specifying the service, path, headers, and body.
 ``httpc_aws:request/5``             | Perform a request specifying the service, method, path, headers, and body.
 ``httpc_aws:request/6``             | Perform a request specifying the service, method, path, headers, body, and ``httpc:http_options().``
 ``httpc_aws:request/7``             | Perform a request specifying the service, method, path, headers, body,  ``httpc:http_options()``, and override the API endpoint. 
 

## Example Usage

The following example assumes that you either have locally configured credentials or that
you're using the AWS Instance Metadata service for credentials:

```erlang
application:start(httpc_aws).
{ok, {Headers, Response}} = httpc_aws:get("ec2","/?Action=DescribeTags&Version=2015-10-01").
```

To configure credentials, invoke ``httpc_aws:set_credentials/2``:

```erlang
application:start(httpc_aws).

httpc:set_credentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"),

RequestHeaders = [{"Content-Type", "application/x-amz-json-1.0"},
                  {"X-Amz-Target", "DynamoDB_20120810.ListTables"}],
                  
{ok, {Headers, Response}} = httpc_aws:post("dynamodb", "/", 
                                           "{\"Limit\": 20}",
                                           RequestHeaders).
```

## Build

```bash
$ bin/rebar3 compile
```

## Test

```bash
$ bin/rebar3 eunit && bin/rebar3 eunit
$ bin/rebar3 dialyzer
```

## License

BSD 3-Clause License
