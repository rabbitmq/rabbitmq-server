# RabbitMQ HTTP Authn/Authz Backend Examples

## Python Example

`rabbitmq_auth_backend_django` is a very minimalistic [Django](https://www.djangoproject.com/) 1.10+ application
that rabbitmq-auth-backend-http can authenticate against. It's really
not designed to be anything other than an example.

### Running the Example

Run `start.sh` to launch it after [installing Django](https://docs.djangoproject.com/en/1.10/topics/install/). You may need to
hack `start.sh` if you are not running Debian or Ubuntu.

The app will use a local SQLite database. It uses the standard
Django authentication database. All users get access to all vhosts and
resources.

### HTTP Endpoint Examples

If you're not familiar with Django, urls.py and auth/views.py may be
most illuminating.


## Spring Boot Example

`rabbitmq_auth_backend_spring_boot` is a simple [Spring Boot](https://projects.spring.io/spring-boot/)
application that rabbitmq-auth-backend-http can authenticate against. It's really
not designed to be anything other than an example.

## Running the Example

Import the example as a Maven project in your favorite IDE or run it directly from the command line:

    mvn spring-boot:run
    
The application listens on the 8080 port.

### HTTP Endpoint Examples

Have a look at the `AuthBackendHttpController`. There's only one user: `guest`,
with the `guest` password. This implementation also checks the
routing key starts with an `a` when publishing to a topic exchange 
or consuming from a topic. (an example of [topic authorisation](http://next.rabbitmq.com/access-control.html#topic-authorisation)).

## ASP.NET Web API Example

`rabbitmq_auth_backend_webapi_dotnet` is a very minimalistic ASP.NET Web API application
the plugin can authenticate against. It's really
**not designed to be anything other than an example**.

### Running the Example

Open the WebApiHttpAuthService.csproj in Visual Studio 2017, More details about prerequisites can be found below.

First, configure RabbitMQ [authn and authz backend](http://www.rabbitmq.com/access-control.html) to use this
plugin using the below config example.

Then Build the solution and run it from Visual Studio.
`Controllers/AuthController.cs` contains the authentication and authorization logic.
By default All users get access to all vhosts and resources.
User "authuser" will be denied access.

### HTTP Endpoint Examples

Have a look at `AuthController`.

### Development Environment

This example was developed using

 * .NET Framework 4.5
 * Visual Studio 2017
 * Windows 10 and IIS v10.0
 
It is possible to build and run service from Visual Studio browse the endpoint without using IIS.
Port number may vary but will likely be `62190`.

When the example is hosted on IIS, port 80 will be used by default.

## PHP Boot Example

`rabbitmq_auth_backend_php` is a simple [PHP](http://php.net/) application that rabbitmq-auth-backend-http can authenticate against.
It's really not designed to be anything other than an example.

### Running the Example

First of all, you need PHP >= 5.4 and [Composer](https://getcomposer.org/) (php package manager).

> ℹ️ The library need PHP >= 5.3, but to run this example you need the PHP >= 5.4 (only because PHP 5.4 as an internal webserver)

The `rabbitmq-auth-backend-http-php` library depend on `symfony/security` and `symfony/http-foundation` components.
Go to the `rabbitmq_auth_backend_php` folder and run `composer install`.

```bash
$ cd rabbitmq_auth_backend_php/
$ composer install
```

Now you can run the PHP 5.4 server (server at http://127.0.0.1:8080)

```
$ composer start
```

Ensure the log file is writable `rabbitmq-auth-backend-http/examples/rabbitmq_auth_backend_php/var/log.log`.

Go to `http://localhost:8080/user.php?username=Anthony&password=anthony-password`, all work properly if you see `Allow administrator` 

### HTTP Endpoint Examples

Have a look at the `bootstrap.php`. By default this example implement the same authorization rules than RabbitMQ.

Users list:

| User | password | is admin | Vhost | Configure regex | Write regex | Read regex | tags |
|--|--|--|--|--|--|--|--|
| Anthony | anthony-password | ✔️ | All | All | All | All | administrator |
| James | bond | | / | .* | .* | .* | management |
| Roger | rabbit | | | | | | monitoring |
| bunny | bugs | | | | | | policymaker |

### rabbitmq.config Example

ℹ️ Dont forget to set the proper url in your rabbit config file

Below is a [RabbitMQ config file](http://www.rabbitmq.com/configure.html) example to go with this
example:

the new format
```
auth_backends.1 = internal  
auth_backends.2 = http
auth_http.user_path     = http://localhost:62190/auth/user.php  
auth_http.vhost_path    = http://localhost:62190/auth/vhost.php  
auth_http.resource_path = http://localhost:62190/auth/resource.php  
auth_http.topic_path    = http://localhost:62190/auth/topic.php
```

Or 

``` erlang
[
  {rabbit, [
              {auth_backends, [rabbit_auth_backend_internal,rabbit_auth_backend_http]}
            ]
  },

{
  rabbitmq_auth_backend_http,
    [
       {http_method,   post},
       {user_path,     "http://localhost:62190/auth/user"},
       {vhost_path,    "http://localhost:62190/auth/vhost"},
       {resource_path, "http://localhost:62190/auth/resource"},
       {topic_path,    "http://localhost:62190/auth/topic"}
    ]
  }
].
```

See [RabbitMQ Access Control guide](http://www.rabbitmq.com/access-control.html) for more information.
