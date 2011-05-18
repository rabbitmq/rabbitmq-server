{application, rabbitmq_shovel_management,
 [{description, "Shovel Status"},
  {vsn, "%%VSN%%"},
  {modules, [rabbit_shovel_mgmt]},
  {registered, []},
  {applications, [kernel, stdlib, rabbit, rabbitmq_management]}]}.
