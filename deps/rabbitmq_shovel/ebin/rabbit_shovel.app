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
  {env, [{defaults, [{qos,            0},
                     {auto_ack,       false},
                     {tx_size,        0},
                     {delivery_mode,  keep},
                     {publish_fields, []},
                     {reconnect,      5}]
         }]},
  {mod, {rabbit_shovel, []}},
  {applications, [kernel, stdlib, rabbit]}]}.
