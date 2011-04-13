{application, rabbitmq_mochiweb,
 [{description, "RabbitMQ Mochiweb Embedding"},
  {vsn, "%%VSN%%"},
  {modules, [
    rabbit_mochiweb,
    rabbit_mochiweb_app,
    rabbit_mochiweb_sup,
    rabbit_mochiweb_web,
    rabbit_mochiweb_registry,
    rabbit_webmachine
  ]},
  {registered, []},
  {mod, {rabbit_mochiweb_app, []}},
  {env, [
        {port, 55672}
        ]},
  {applications, [kernel, stdlib]}]}.
