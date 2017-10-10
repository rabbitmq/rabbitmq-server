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

## DotNet WebApi Example

`rabbitmq_auth_backend_webapi_dotnet` is a very minimalistic DotNet WebApi application
that rabbitmq-auth-backend-http can authenticate against. It's really
not designed to be anything other than an example.

### Running the Example

Open the WebApiHttpAuthService.csproj in Visual Studio 2017, More details are giving in below section "Where was this sample tested"
Build the solution and run it from Visual Studio, Then make changes to rabbitmq.config file for the URL or HTTPEndpoints refer to section below "rabbitmq.config file changes"
You can write custom logic inside Controllers/AuthController.cs, By default All users get access to all vhosts and
resources. An example of deny is also given for one user "authuser".

### HTTP Endpoint Examples

Have a look at the `AuthController`.

### Where was this sample tested
DotNetFramework 4.5
Visual Studio 2017
Windows10, This application is Hosted on IIS V10.0
if not using IIS, build and run service from Visual Studio browse the endpoint
something like
http://localhost:62190 port number-62190 might vary, 
If hosted on IIS and using the default port 80, then port number might not be required 

### rabbitmq.config file changes
Copy url from the browsed website and put it in rabbitmq.config file
like below

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

Now it should start authenticating/authorizing from the new http service if not found in rabbit_auth_backend_internal configuration
