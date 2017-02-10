# RabbitMQ trust store HTTP server example

`rabbitmq_trust_store_django` is a very minimalistic [Django](https://www.djangoproject.com/) 1.10+ application
that rabbitmq-trust-store's `rabbit_trust_store_http_provider` can use as a source of certificates.
The project serves PEM encoded CA certificate files from the `certs` directory.
It's really not designed to be anything other than an example.


## Running the Example

1. Put certificates that should be trusted in PEM format into the `certs` directory.

2. Run `python manage.py runserver` to launch it after [installing Django](https://docs.djangoproject.com/en/1.10/topics/install/).

3. Configure RabbitMQ trust store to use `rabbit_trust_store_http_provider`.

Below is a very minimalistic example that assumes the example is available
on the local machine via HTTP:

```
{rabbitmq_trust_store,
 [{providers, [rabbit_trust_store_http_provider]},
  {url, "http://127.0.0.1:8000/"},
  {refresh_interval, {seconds, 30}}
 ]}.
```

You can specify TLS options if you use HTTPS:

```
{rabbitmq_trust_store,
 [{providers, [rabbit_trust_store_http_provider]},
  {url, "https://secure.store.example.local:8000/"},
  {refresh_interval, {seconds, 30}},
  {ssl_options, [{certfile, "/client/cert.pem"},
                 {keyfile, "/client/key.pem"},
                 {cacertfile, "/ca/cert.pem"}
                ]}
 ]}.
```


## HTTP API Endpoints

This project will serve static files from `certs` directory and
will list them in JSON format described in [rabbitmq-trust-store](https://github.com/rabbitmq/rabbitmq-trust-store/)

If you're not familiar with Django, urls.py and auth/views.py may be
most illuminating.
