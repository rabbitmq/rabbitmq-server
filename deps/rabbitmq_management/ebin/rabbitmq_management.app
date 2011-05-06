{application, rabbitmq_management,
 [{description, "RabbitMQ Management Console"},
  {vsn, "%%VSN%%"},
  %% TODO generate automatically. NB: _app needed!
  {modules, [rabbit_mgmt_app,
             rabbit_mgmt_dispatcher]},
  {registered, []},
  {mod, {rabbit_mgmt_app, []}},
  {env, [{http_log_dir, none}]},
  {applications, [kernel, stdlib, rabbit, rabbitmq_mochiweb, webmachine,
                  amqp_client, rabbitmq_management_agent]}]}.
