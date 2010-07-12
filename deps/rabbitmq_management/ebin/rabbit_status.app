{application, rabbit_status,
 [{description, "RabbitMQ Status Page"},
  {vsn, "0.01"},
  {modules, [
    rabbit_status,
    rabbit_status_app,
    rabbit_status_sup,
    rabbit_status_web
  ]},
  {registered, []},
  {mod, {rabbit_status_app, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbit, rabbit_mochiweb]}]}.
