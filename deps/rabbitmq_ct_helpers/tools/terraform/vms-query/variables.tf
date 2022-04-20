# vim:sw=2:et:

variable "uuid" {
  description = <<EOF
Unique ID of the deployment.
EOF
}

variable "erlang_nodename" {
  default     = "unspecified"
  description = <<EOF
Name of the remote Erlang node.
EOF
}
