{application, rabbit_management,
 [{description, "RabbitMQ Management Console"},
  {vsn, "0.01"},
  {modules, [
    rabbit_management,
    rabbit_management_app,
    rabbit_management_sup,
    rabbit_management_cache
  ]},
  {registered, []},
  {mod, {rabbit_management_app, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbit, rabbit_mochiweb]}]}.
