{application, rabbitmq_management_agent,
 [{description, "RabbitMQ Management Agent"},
  {vsn, "%%VSN%%"},
  {modules, [rabbit_mgmt_agent_app,
             rabbit_mgmt_db_handler]}, %% TODO generate automatically.
  {registered, []},
  {mod, {rabbit_mgmt_agent_app, []}},
  {env, [{force_fine_statistics, true}]},
  {applications, [kernel, stdlib, rabbit]}]}.
