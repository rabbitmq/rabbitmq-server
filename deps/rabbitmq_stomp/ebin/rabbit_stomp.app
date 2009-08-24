{application, rabbit_stomp,
 [{description, "Embedded Rabbit Stomp Adapter"},
  {vsn, "0.01"},
  {modules, [
    rabbit_stomp_sup,
    stomp_frame,
    rabbit_stomp
  ]},
  {registered, []},
  {mod, {rabbit_stomp_sup, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbit, amqp_client]}]}.
