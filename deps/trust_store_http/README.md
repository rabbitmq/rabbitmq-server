# Example Trust Store HTTP Server for RabbitMQ

This tiny HTTP server serves CA certificates from a user-specified local directory.
It is meant to be used with [RabbitMQ trust store plugin](https://github.com/rabbitmq/rabbitmq-trust-store)
in its test suite and as an example.

## Endpoints

 * `/`: serves a list of certificates in JSON. The format is `{"certificates":[{"id": <id>, "path": <path>}, ...]}`
 * `/certs/<file_name>`: access for PEM encoded certificate files
 * `/invlid`: serves invalid JSON, to be used in integration tests

```
<id> = <file_name>:<file_modification_date>
<path> = /certs/<file_name>
<file_name> = name of a PEM file in the listed directory
```

## Usage

To rebuild and run a release (requires Erlang to be installed):

```
gmake run CERT_DIR="/my/cacert/directory" PORT=8080
```

To run from the pre-built escript (requires Erlang to be installed):

```
gmake
CERT_DIR="/my/cacert/directory" PORT=8080 ./_rel/trust_store_http_release/bin/trust_store_http_release console
```


## HTTPS

To start an HTTPS server, you should provide ssl options. It can be done via
Erlang `.config` file format:

```
[{trust_store_http,
    [{ssl_options, [{cacertfile,"/path/to/testca/cacert.pem"},
                    {certfile,"/path/to/server/cert.pem"},
                    {keyfile,"/path/to/server/key.pem"},
                    {verify,verify_peer},
                    {fail_if_no_peer_cert,false}]}]}]
```


This configuration can be added to `rel/sys.config`
if you're running the application from source `make run`

Or it can be specified as an environment variable:

```
CERT_DIR="/my/cacert/directory" PORT=8443 CONFIG_FILE=my_config.config ./_rel/trust_store_http_release/bin/trust_store_http_release console
```

Port and directory can be also set via config file:


```
[{trust_store_http,
    [{directory, "/tmp/certs"},
     {port, 8081},
     {ssl_options, [{cacertfile,"/path/to/testca/cacert.pem"},
                    {certfile,"/path/to/server/cert.pem"},
                    {keyfile,"/path/to/server/key.pem"},
                    {verify,verify_peer},
                    {fail_if_no_peer_cert,false}]}]}]
```
