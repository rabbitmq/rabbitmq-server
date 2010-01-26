{application, rabbit_shovel,
 [{description, "Data Shovel for RabbitMQ"},
  {vsn, "0.01"},
  {modules, [
    supervisor3,
    rabbit_shovel,
    rabbit_shovel_sup,
    rabbit_shovel_worker
  ]},
  {registered, []},
  {env, []},
  {mod, {rabbit_shovel, []}},
  {applications, [kernel, stdlib, rabbit]}]}.
