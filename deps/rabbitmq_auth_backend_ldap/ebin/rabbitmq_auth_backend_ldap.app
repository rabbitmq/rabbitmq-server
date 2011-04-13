%% -*- erlang -*-
{application, rabbitmq_auth_backend_ldap,
 [{description, "RabbitMQ LDAP Authentication Backend"},
  {vsn, "%%VSN%%"},
  {modules, [rabbit_auth_backend_ldap_app]}, %% TODO generate automatically.
  {registered, []},
  {mod, {rabbit_auth_backend_ldap_app, []}},
  {env, [ {servers,               ["ldap"]},
          {user_dn_pattern,       "cn=${username},ou=People,dc=example,dc=com"},
          {other_bind,            anon},
          {vhost_access_query,    {constant, true}},
          {resource_access_query, {constant, true}},
          {is_admin_query,        {constant, false}},
          {use_ssl,               false},
          {port,                  389},
          {log,                   false} ] },
  {applications, [kernel, stdlib]}]}.
