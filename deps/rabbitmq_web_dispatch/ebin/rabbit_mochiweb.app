{application, rabbit_mochiweb,
 [{description, "RabbitMQ Mochiweb Embedding"},
  {vsn, "0.01"},
  {modules, [
    rabbit_mochiweb,
    rabbit_mochiweb_app,
    rabbit_mochiweb_sup,
    rabbit_mochiweb_web
  ]},
  {registered, []},
  {mod, {rabbit_mochiweb_app, []}},
  {env, [
        {port, 8000}
        ]},
  {applications, [kernel, stdlib]}]}.
