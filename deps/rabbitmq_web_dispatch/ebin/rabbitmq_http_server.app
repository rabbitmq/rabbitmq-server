{application, rabbitmq_http_server,
 [{description, "RabbitMQ HTTP Server"},
  {vsn, "0.01"},
  {modules, [
    rabbitmq_http_server,
    rabbitmq_http_server_app,
    rabbitmq_http_server_sup,
    rabbitmq_http_server_web
  ]},
  {registered, []},
  {mod, {rabbitmq_http_server_app, []}},
  {env, [
        {port, 8000}
        ]},
  {applications, [kernel, stdlib]}]}.
