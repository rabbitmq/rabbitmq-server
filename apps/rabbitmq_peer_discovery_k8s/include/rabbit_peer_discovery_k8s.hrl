%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(UTIL_MODULE,   rabbit_peer_discovery_util).
-define(HTTPC_MODULE,  rabbit_peer_discovery_httpc).

-define(BACKEND_CONFIG_KEY, peer_discovery_k8s).

-define(K8S_EVENT_SOURCE_DESCRIPTION, "rabbitmq_peer_discovery").

-define(CONFIG_MAPPING,
         #{
          k8s_scheme                         => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_SCHEME",
                                                   default_value = "https"
                                                  },
          k8s_host                           => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_HOST",
                                                   default_value = "kubernetes.default.svc.cluster.local"
                                                  },
          k8s_port                           => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "K8S_PORT",
                                                   default_value = 443
                                                  },
          k8s_token_path                     => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_TOKEN_PATH",
                                                   default_value = "/var/run/secrets/kubernetes.io/serviceaccount/token"
                                                  },
          k8s_cert_path                      => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_CERT_PATH",
                                                   default_value = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
                                                  },
          k8s_namespace_path                 => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_NAMESPACE_PATH",
                                                   default_value = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
                                                  },
          k8s_service_name                   => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_SERVICE_NAME",
                                                   default_value = "rabbitmq"
                                                  },
          k8s_address_type                   => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_ADDRESS_TYPE",
                                                   default_value = "ip"
                                                  },
          k8s_hostname_suffix                => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "K8S_HOSTNAME_SUFFIX",
                                                   default_value = ""
                                                  }
         }).
