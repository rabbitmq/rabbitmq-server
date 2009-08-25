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
  {env, [{listeners, [{"127.0.0.1", 61613}]}]},
  {applications, [kernel, stdlib, rabbit, amqp_client]}]}.
