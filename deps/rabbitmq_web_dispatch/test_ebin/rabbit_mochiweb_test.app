{application, rabbit_mochiweb_test,
 [{description, "rabbit_mochiweb_test"},
  {vsn, "0.01"},
  {modules, [
                rabbit_mochiweb_test_app
  ]},
  {registered, []},
  {mod, {rabbit_mochiweb_test_app, []}},
  {env, []},
  {applications, [kernel, stdlib, rabbit_mochiweb]}]}.
