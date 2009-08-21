{application, amqp_client,
 [{description, "AMQP Client"},
  {vsn, "1.6.0"},
  {modules, [
             amqp_channel,
             amqp_connection,
             amqp_direct_driver,
             amqp_network_driver,
             amqp_rpc_client,
             amqp_rpc_server
  ]},
  {registered, []},
  {env, []},
  {applications, [kernel, stdlib]}]}.
