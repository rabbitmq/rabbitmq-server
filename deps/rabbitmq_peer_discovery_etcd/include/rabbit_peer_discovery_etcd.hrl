-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(UTIL_MODULE,   rabbit_peer_discovery_util).
-define(HTTPC_MODULE,  rabbit_peer_discovery_httpc).

-define(BACKEND_CONFIG_KEY, peer_discovery_etcd).


-define(CONFIG_MAPPING,
         #{
          etcd_scheme    => #peer_discovery_config_entry_meta{
                              type          = string,
                              env_variable  = "ETCD_SCHEME",
                              default_value = "http"
                            },
          etcd_host      => #peer_discovery_config_entry_meta{
                              type          = string,
                              env_variable  = "ETCD_HOST",
                              default_value = "localhost"
                            },
          etcd_port      => #peer_discovery_config_entry_meta{
                              type          = integer,
                              env_variable  = "ETCD_PORT",
                              default_value = 2379
                            },
          etcd_prefix    => #peer_discovery_config_entry_meta{
                              type          = string,
                              env_variable  = "ETCD_PREFIX",
                              default_value = "rabbitmq"
                            },
          etcd_node_ttl  => #peer_discovery_config_entry_meta{
                              type          = integer,
                              env_variable  = "ETCD_NODE_TTL",
                              default_value = 30
                            },
          cluster_name   => #peer_discovery_config_entry_meta{
                              type          = string,
                              env_variable  = "CLUSTER_NAME",
                              default_value = "default"
                            },
          lock_wait_time  => #peer_discovery_config_entry_meta{
                                type          = integer,
                                env_variable  = "LOCK_WAIT_TIME",
                                default_value = 300
                               }
         }).

