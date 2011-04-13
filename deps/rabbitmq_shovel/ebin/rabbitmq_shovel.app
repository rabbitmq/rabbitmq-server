{application, rabbitmq_shovel,
 [{description, "Data Shovel for RabbitMQ"},
  {vsn, "%%VSN%%"},
  {modules, [
    rabbit_shovel,
    rabbit_shovel_status,
    rabbit_shovel_sup,
    rabbit_shovel_worker,
    uri_parser
  ]},
  {registered, []},
  {env, [{defaults, [{prefetch_count,     0},
                     {auto_ack,           false},
                     {tx_size,            0},
                     {publish_fields,     []},
                     {publish_properties, []},
                     {reconnect_delay,    5}]
         }]},
  {mod, {rabbit_shovel, []}},
  {applications, [kernel, stdlib, rabbit]}]}.
