{application, rabbit_management,
 [{description, "RabbitMQ Status Page"},
  {vsn, "0.01"},
  {modules, [
    rabbit_management,
    rabbit_management_app,
    rabbit_management_sup,
    rabbit_management_web
  ]},
  {registered, []},
  {mod, {rabbit_management_app, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbit, rabbit_mochiweb]}]}.
