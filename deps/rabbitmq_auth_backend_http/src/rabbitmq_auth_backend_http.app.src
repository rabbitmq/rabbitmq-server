%% -*- erlang -*-
{application, rabbitmq_auth_backend_http,
 [{description, "RabbitMQ HTTP Authentication Backend"},
  {vsn, "%%VSN%%"},
  {modules, [rabbit_auth_backend_http_app]},
  {registered, []},
  {mod, {rabbit_auth_backend_http_app, []}},
  {env, [{user_path,     "http://localhost:8000/auth/user"},
         {vhost_path,    "http://localhost:8000/auth/vhost"},
         {resource_path, "http://localhost:8000/auth/resource"}] },
  {applications, [kernel, stdlib, inets]}]}.
