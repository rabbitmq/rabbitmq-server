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
  default     = "control"
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

variable "erlang_version_to_system" {
  type = map
  default = {
    "R16B03" = "debian-wheezy"
    "17.5"   = "debian-jessie"
    "18.3"   = "debian-jessie"
    "19.3"   = "debian-jessie"
    "20.0"   = "debian-stretch"
    "20.1"   = "debian-stretch"
    "20.2"   = "debian-stretch"
    "20.3"   = "debian-stretch"
    "21.0"   = "debian-stretch"
    "21.1"   = "debian-stretch"
    "21.2"   = "debian-stretch"
    "21.3"   = "debian-stretch"
    "22.0"   = "debian-stretch"
    "22.1"   = "debian-stretch"
    "22.2"   = "debian-stretch"
    "22.3"   = "debian-stretch"
    "23.0"   = "debian-stretch"
    "23.1"   = "debian-stretch"
    "23.2"   = "debian-stretch"
    "23.3"   = "debian-buster"
    "24.0"   = "debian-buster"
  }
}

variable "ec2_instance_types" {
  type = map
  default = {
  }
}

# AMIs for eu-west-1 (Ireland)
variable "amis" {
  type = map
  default = {
    "centos-7"           = "ami-6e28b517"
    "centos-8"           = "ami-0645e7b5435a343a5" # Community-provided
    "debian-buster"      = "ami-02498d1ddb8cc6a86" # Community-provided
    "fedora-30"          = "ami-0c8df718af40abdae"
    "fedora-31"          = "ami-00d8194a6e394e1c5"
    "fedora-32"          = "ami-0f17c0eb4a2e08778"
    "fedora-33"          = "ami-0aa3a65f84cb982ca"
    "freebsd-10"         = "ami-76f82c0f"
    "freebsd-11"         = "ami-ab56bed2"
    "opensuse-leap-15.1" = "ami-0f81506cab2b62029"
    "opensuse-leap-15.2" = "ami-013f2b687f5a91567"
    "rhel-7"             = "ami-8b8c57f8"
    "sles-11"            = "ami-a2baf5d5"
    "sles-12"            = "ami-f4278487"
    "ubuntu-16.04"       = "ami-067b6923c66564bf6"
    "ubuntu-18.04"       = "ami-01cca82393e531118"
    "ubuntu-20.04"       = "ami-09376517f0f510ad9"
  }
}

variable "usernames" {
  type = map
  default = {
    "centos-7"           = "centos"
    "centos-8"           = "centos"
    "debian-buster"      = "admin"
    "debian-bullseye"    = "admin"
    "fedora-30"          = "fedora"
    "fedora-31"          = "fedora"
    "fedora-32"          = "fedora"
    "fedora-33"          = "fedora"
    "freebsd-10"         = "ec2-user"
    "freebsd-11"         = "ec2-user"
    "opensuse-leap-15.1" = "ec2-user"
    "opensuse-leap-15.2" = "ec2-user"
    "rhel-7"             = "ec2-user"
    "sles-11"            = "ec2-user"
    "sles-12"            = "ec2-user"
    "ubuntu-16.04"       = "ubuntu"
    "ubuntu-18.04"       = "ubuntu"
    "ubuntu-20.04"       = "ubuntu"
  }
}
