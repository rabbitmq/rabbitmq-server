{application, rabbit_mochiweb,
 [{description, "RabbitMQ Mochiweb Embedding"},
  {vsn, "0.01"},
  {modules, [
    rabbit_mochiweb,
    rabbit_mochiweb_app,
    rabbit_mochiweb_sup,
    rabbit_mochiweb_web,
    rabbit_mochiweb_registry
  ]},
  {registered, []},
  {mod, {rabbit_mochiweb_app, []}},
  {env, [
        {port, 55672}
        ]},
  {applications, [kernel, stdlib]}]}.
