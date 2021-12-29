# vim:sw=2:et:

variable "erlang_version" {
  description = <<EOF
Erlang version to deploy on VMs. This may also determine the version of
the underlying OS.
EOF
}

variable "erlang_git_ref" {
  default     = ""
  description = <<EOF
Git reference if building Erlang from Git. Specifying the Erlang
version is still required.
EOF
}

variable "elixir_version" {
  default     = ""
  description = <<EOF
Elixir version to deploy on VMs. Default to the latest available.
EOF
}

variable "erlang_cookie" {
  description = <<EOF
Erlang cookie to deploy on VMs.
EOF
}

variable "erlang_nodename" {
  description = <<EOF
Name of the remote Erlang node.
EOF
}

variable "ssh_key" {
  description = <<EOF
Path to the private SSH key to use to communicate with the VMs. The
module then assumes that the public key is named "$ssh_key.pub".
EOF
}

variable "instance_count" {
  default     = "1"
  description = <<EOF
Number of VMs to spawn.
EOF
}

variable "upload_dirs_archive" {
  description = <<EOF
Archive of the directories to upload to the VMs. They will be placed
in / on the VM, which means that the paths can be identical.
EOF
}

variable "instance_name_prefix" {
  default = "RabbitMQ testing: "
}

variable "instance_name_suffix" {
  default = ""
}

variable "instance_name" {
  default = "Unnamed"
}

variable "vpc_cidr_block" {
  default = "10.0.0.0/16"
}

variable "files_suffix" {
  default = ""
}

variable "aws_ec2_region" {
  default = "eu-west-1"
}
