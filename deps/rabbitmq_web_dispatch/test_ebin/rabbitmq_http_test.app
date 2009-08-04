{application, rabbitmq_http_test,
 [{description, "rabbitmq_http_test"},
  {vsn, "0.01"},
  {modules, [
                rabbitmq_http_test_app
  ]},
  {registered, []},
  {mod, {rabbitmq_http_test_app, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbitmq_http_server]}]}.
