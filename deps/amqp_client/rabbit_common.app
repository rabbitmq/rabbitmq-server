{application, rabbit_common,
 [{description, "RabbitMQ Common Libraries"},
  {vsn, "1.6.0"},
  {modules, [
             rabbit_writer,
             rabbit_reader,
             rabbit_framing,
             rabbit_framing_channel,
             rabbit_binary_parser,
             rabbit_binary_generator,
             rabbit_channel,
             rabbit_misc,
             rabbit_heartbeat,
             gen_server2
  ]},
  {registered, []},
  {env, []},
  {applications, [kernel, stdlib]}]}.
