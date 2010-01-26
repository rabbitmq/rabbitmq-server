{application, rabbit_shovel,
 [{description, "Data Shovel for RabbitMQ"},
  {vsn, "0.01"},
  {modules, [
    supervisor2
  ]},
  {registered, []},
  {env, []},
  {applications, [kernel, stdlib, rabbit]}]}.
