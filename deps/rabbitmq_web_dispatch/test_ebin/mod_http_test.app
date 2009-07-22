{application, mod_http_test,
 [{description, "mod_http_test"},
  {vsn, "0.01"},
  {modules, [
                mod_http_test
  ]},
  {registered, []},
  {mod, {mod_http_test, []}},
  {env, []},
  {applications, [kernel, stdlib, mod_http, rfc4627_jsonrpc]}]}.
