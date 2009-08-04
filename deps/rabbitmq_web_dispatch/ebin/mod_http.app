{application, mod_http,
 [{description, "mod_http"},
  {vsn, "0.01"},
  {modules, [
    mod_http,
    mod_http_app,
    mod_http_sup,
    mod_http_web,
    mod_http_deps
  ]},
  {registered, []},
  {mod, {mod_http_app, []}},
  {env, [
        {port, 8000},
        {production, true}
        ]},
  {applications, [kernel, stdlib]}]}.
