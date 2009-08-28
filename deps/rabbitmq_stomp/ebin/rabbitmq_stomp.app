{application, rabbitmq_stomp,
 [{description, "Embedded Rabbit Stomp Adapter"},
  {vsn, "0.01"},
  {modules, [
    rabbitmq_stomp,
    rabbitmq_stomp_sup,
    stomp_server,
    stomp_frame
  ]},
  {registered, []},
  {mod, {rabbitmq_stomp, []}},
  {env, [{listeners, [{"127.0.0.1", 61613}]}]},
  {applications, [kernel, stdlib, rabbit, amqp_client]}]}.
