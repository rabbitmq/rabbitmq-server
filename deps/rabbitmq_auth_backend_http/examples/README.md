# RabbitMQ HTTP Authn/Authz Backend Example

`rabbitmq_auth_backend_django` is a very minimalistic [Django](https://www.djangoproject.com/) 1.10+ application
that rabbitmq-auth-backend-http can authenticate against. It's really
not designed to be anything other than an example.

## Running the Example

Run `start.sh` to launch it after [installing Django](https://docs.djangoproject.com/en/1.10/topics/install/). You may need to
hack `start.sh` if you are not running Debian or Ubuntu.

The app will use a local SQLite database. It uses the standard
Django authentication database. All users get access to all vhosts and
resources.

## HTTP Endpoint Examples

If you're not familiar with Django, urls.py and auth/views.py may be
most illuminating.
