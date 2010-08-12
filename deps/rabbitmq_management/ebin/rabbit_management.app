{application, rabbit_management,
 [{description, "RabbitMQ Management Console"},
  {vsn, "0.01"},
  {modules, [
    rabbit_mgmt,
    rabbit_mgmt_app,
    rabbit_mgmt_sup,
    rabbit_mgmt_cache
  ]},
  {registered, []},
  {mod, {rabbit_mgmt_app, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbit, rabbit_mochiweb]}]}.
