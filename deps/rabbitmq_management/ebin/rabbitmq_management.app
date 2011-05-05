{application, rabbitmq_management,
 [{description, "RabbitMQ Management Console"},
  {vsn, "%%VSN%%"},
  {modules, [rabbit_mgmt_app]}, %% TODO generate automatically. NB: _app needed!
  {registered, []},
  {mod, {rabbit_mgmt_app, []}},
  {env, [{http_log_dir, none},
         {plugins,      []}]},
  {applications, [kernel, stdlib, rabbit, rabbitmq_mochiweb, webmachine,
                  amqp_client, rabbitmq_management_agent]}]}.
