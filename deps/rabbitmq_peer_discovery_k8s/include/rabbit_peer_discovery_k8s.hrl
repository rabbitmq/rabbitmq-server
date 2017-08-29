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
