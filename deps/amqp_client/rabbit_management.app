{application, rabbit_management,
 [{description, "Rabbit Management"},
  {id, "Rabbit Management"},
  {vsn, "0.1"},
  {modules, [
            rabbit_management_supervisor
             ]},
  {registered, [rabbit_management_supervisor]},
  {applications, [kernel, stdlib, sasl, rabbit]},
  {mod, {rabbit_management_application, []}},
  {env, []}
 ]
}.
