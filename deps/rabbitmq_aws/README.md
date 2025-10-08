# rabbitmq-aws

A fork of [gmr/httpc-aws](https://github.com/gmr/httpc-aws) for use in building RabbitMQ plugins that interact with Amazon Web Services APIs.

## Supported Erlang Versions

[Same as RabbitMQ](http://www.rabbitmq.com/which-erlang.html)

## Configuration

Configuration for *rabbitmq-aws* is can be provided in multiple ways. It is designed
to behave similarly to the [AWS Command Line Interface](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
with respect to providing region and configuration information. Additionally it
has two methods, `rabbitmq_aws:set_region/1` and `rabbitmq_aws:set_credentials/2`
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

### EC2 Instance Metadata Service Versions

There are two versions of the EC2 Instance Metadata Service (IMDS) that are available by default on EC2 instances; IMDSv1 and IMDSv2 which is protected by session authentication
and [adds defenses against additional vulnerabilities](https://aws.amazon.com/blogs/security/defense-in-depth-open-firewalls-reverse-proxies-ssrf-vulnerabilities-ec2-instance-metadata-service/).
AWS recommends adopting IMDSv2 and disabling IMDSv1 [by configuring the Instance Metadata Service on the EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html).

By default *rabbitmq-aws* will attempt to use IMDSv2 first and will fallback to use IMDSv1 if calls to IMDSv2 fail. This behavior can be overridden
by setting the `aws.prefer_imdsv2` setting to `false`.

### Environment Variables

As with the AWS CLI, the following environment variables can be used to provide
configuration or to impact configuration behavior:

 - `AWS_DEFAULT_PROFILE`
 - `AWS_DEFAULT_REGION`
 - `AWS_CONFIG_FILE`
 - `AWS_SHARED_CREDENTIALS_FILE`
 - `AWS_ACCESS_KEY_ID`
 - `AWS_SECRET_ACCESS_KEY`

## API Functions

  Method                                    | Description
 -------------------------------------------|--------------------------------------------------------------------------------------------
 `rabbitmq_aws:set_region/1`                | Manually specify the AWS region to make requests to.
 `rabbitmq_aws:set_credentials/2`           | Manually specify the request credentials to use.
 `rabbitmq_aws:refresh_credentials/0`       | Refresh the credentials from the environment, filesystem, or EC2 Instance Metadata Service.
 `rabbitmq_aws:ensure_imdsv2_token_valid/0` | Make sure EC2 IMDSv2 token is active and valid.
 `rabbitmq_aws:get/2`                       | Perform a GET request to the API specifying the service and request path.
 `rabbitmq_aws:get/3`                       | Perform a GET request specifying the service, path, and headers.
 `rabbitmq_aws:post/4`                      | Perform a POST request specifying the service, path, headers, and body.
 `rabbitmq_aws:request/5`                   | Perform a request specifying the service, method, path, headers, and body.
 `rabbitmq_aws:request/6`                   | Perform a request specifying the service, method, path, headers, body, and `httpc:http_options().`
 `rabbitmq_aws:request/7`                   | Perform a request specifying the service, method, path, headers, body,  `httpc:http_options()`, and override the API endpoint.
 `rabbitmq_aws:api_get_request/2`           | Perform an AWS service API request with retries.
 `rabbitmq_aws:api_post_request/2`          | Perform an AWS service API request with retries.

## Example Usage

The following example assumes that you either have locally configured credentials or that
you're using the EC2 Instance Metadata Service for credentials:

```erlang
application:start(rabbitmq_aws).
{ok, {Headers, Response}} = rabbitmq_aws:get("ec2","/?Action=DescribeTags&Version=2015-10-01").
```
To configure credentials, invoke `rabbitmq_aws:set_credentials/2`:

```erlang
application:start(rabbitmq_aws).

rabbitmq_aws:set_credentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"),

RequestHeaders = [{"Content-Type", "application/x-amz-json-1.0"},
                  {"X-Amz-Target", "DynamoDB_20120810.ListTables"}],

{ok, {Headers, Response}} = rabbitmq_aws:post("dynamodb", "/",
                                              "{\"Limit\": 20}",
                                              RequestHeaders).
```

## Build

```bash
make
```

## Test

```bash
make tests
```

## License

BSD 3-Clause License
