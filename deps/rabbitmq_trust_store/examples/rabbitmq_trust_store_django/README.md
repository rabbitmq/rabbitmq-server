# RabbitMQ trust store HTTP server example

`rabbitmq_trust_store_django` is a very minimalistic [Django](https://www.djangoproject.com/) 1.10+ application
that rabbitmq-trust-store's `rabbit_trust_store_http_provider` can use as a source of certificates.
The project serves PEM encoded CA certificate files from the `certs` directory.
It's really not designed to be anything other than an example.

## Running the Example

1. Put the CA certificates in PEM format to `certs` directory.

2. Run `python manage.py runserver` to launch it after [installing Django](https://docs.djangoproject.com/en/1.10/topics/install/).

3. Set up rabbitmq-trust-store to use `rabbit_trust_store_http_provider` and


## HTTP Endpoints

This project will serve static files from `certs` directory and
will list them in JSON format described in [rabbitmq-trust-store](https://github.com/rabbitmq/rabbitmq-trust-store/)

If you're not familiar with Django, urls.py and auth/views.py may be
most illuminating.
