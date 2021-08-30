%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(BACKEND_CONFIG_KEY, peer_discovery_consul).

-define(CONFIG_MAPPING,
         #{
          cluster_name                       => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CLUSTER_NAME",
                                                   default_value = "default"
                                                  },
          consul_acl_token                   => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_ACL_TOKEN",
                                                   default_value = "undefined"
                                                  },
          consul_include_nodes_with_warnings => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_INCLUDE_NODES_WITH_WARNINGS",
                                                   default_value = false
                                                  },
          consul_scheme                      => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SCHEME",
                                                   default_value = "http"
                                                  },
          consul_host                        => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_HOST",
                                                   default_value = "localhost"
                                                  },
          consul_port                        => #peer_discovery_config_entry_meta{
                                                   type          = port,
                                                   env_variable  = "CONSUL_PORT",
                                                   default_value = 8500
                                                  },
          consul_domain                      => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_DOMAIN",
                                                   default_value = "consul"
                                                  },
          consul_svc                         => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SVC",
                                                   default_value = "rabbitmq"
                                                  },
          consul_svc_addr                    => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SVC_ADDR",
                                                   default_value = "undefined"
                                                  },
          consul_svc_addr_auto               => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_SVC_ADDR_AUTO",
                                                   default_value = false
                                                  },
          consul_svc_addr_nic                => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SVC_ADDR_NIC",
                                                   default_value = "undefined"
                                                  },
          consul_svc_addr_nodename           => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_SVC_ADDR_NODENAME",
                                                   default_value = false
                                                  },
          consul_svc_port                    => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "CONSUL_SVC_PORT",
                                                   default_value = 5672
                                                  },
          consul_svc_ttl                     => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "CONSUL_SVC_TTL",
                                                   default_value = 30
                                                  },
          consul_svc_tags                    => #peer_discovery_config_entry_meta{
                                                   type          = list,
                                                   env_variable  = "CONSUL_SVC_TAGS",
                                                   default_value = []
                                                  },
          consul_svc_meta                    => #peer_discovery_config_entry_meta{
                                                   type          = list,
                                                   default_value = []
                                                  },
          consul_deregister_after            => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "CONSUL_DEREGISTER_AFTER",
                                                   default_value = ""
                                                  },
          consul_use_longname                => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_USE_LONGNAME",
                                                   default_value = false
                                                  },
          consul_lock_prefix                 => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_LOCK_PREFIX",
                                                   default_value = "rabbitmq"
                                                  },
          lock_wait_time                     => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "LOCK_WAIT_TIME",
                                                   default_value = 300
                                                  }
         }).
