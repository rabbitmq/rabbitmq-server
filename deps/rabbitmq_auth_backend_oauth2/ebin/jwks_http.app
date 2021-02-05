{application, 'jwks_http', [
                            {description, "Dummy JWKS server"},
                            {vsn, "0.0.0"},
                            {modules, ['jwks_http_app','jwks_http_sup','jwks_http_handler']},
                            {mod, {jwks_http_app, undefined}},
                            {registered, []},
                            {applications, [kernel,stdlib,jsx,cowboy]},
                            {env, []}
                           ]}.
