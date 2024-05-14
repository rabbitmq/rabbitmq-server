log_level = "DEBUG"
enable_syslog = false
enable_script_checks = false
enable_local_script_checks = true

datacenter = "dc1"
server = true
bootstrap_expect = 1

## ACL configuration
acl = {
  enabled = true
  default_policy = "allow"
  enable_token_persistence = true
  enable_token_replication = true
  down_policy = "extend-cache"
}

# Enable service mesh
connect {
  enabled = true
}

# Addresses and ports
client_addr = "0.0.0.0"
bind_addr   = "0.0.0.0"

addresses {
  grpc = "0.0.0.0"
  http = "0.0.0.0"
}
