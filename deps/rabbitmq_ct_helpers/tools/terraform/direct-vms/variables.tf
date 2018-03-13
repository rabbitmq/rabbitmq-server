# vim:sw=2:et:

variable "erlang_version" {
  description = <<EOF
Erlang version to deploy on VMs. This may also determine the version of
the underlying OS.
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
  type = "map"
  default = {
    "R16B03" = "debian-wheezy"
    "17.5"   = "debian-jessie"
    "18.3"   = "debian-jessie"
    "19.3"   = "debian-jessie"
    "20.0"   = "debian-stretch"
    "20.1"   = "debian-stretch"
    "20.2"   = "debian-stretch"
  }
}

variable "ec2_instance_types" {
  type = "map"
  default = {
    "sles-11"      = "t2.medium" # Need more than 2 GiB of RAM
    "ubuntu-14.04" = "m3.medium" # `t2.micro` unsupported
    "ubuntu-16.10" = "m3.medium" # `t2.micro` unsupported
    "ubuntu-17.04" = "m3.medium" # `t2.micro` unsupported
    "ubuntu-17.10" = "m3.medium" # `t2.micro` unsupported
  }
}

# AMIs for eu-west-1 (Ireland)
variable "amis" {
  type = "map"
  default = {
    "centos-6"            = "ami-051b1563"
    "centos-7"            = "ami-061b1560"
    "debian-wheezy"       = "ami-61e56916"
    "debian-jessie"       = "ami-3291be54"
    "debian-stretch"      = "ami-907f9ae9"
    "fedora-24"           = "ami-415ec132" # Community image.
    "fedora-25"           = "ami-ffe8b88c" # Community image.
    "fedora-26"           = "ami-aac928d3" # Community image.
    "freebsd-10"          = "ami-809012f3"
    "freebsd-11"          = "ami-ab56bed2"
    "opensuse-leap-42.2"  = "ami-8bfda0ed"
    "rhel-6"              = "ami-c1bb06b2"
    "rhel-7.0"            = "ami-8cff51fb"
    "rhel-7.1"            = "ami-25158352"
    "rhel-7.2"            = "ami-8b8c57f8"
    "sles-11"             = "ami-a2baf5d5"
    "sles-12"             = "ami-f4278487"
    "ubuntu-12.04"        = "ami-ee0b0688" # Community image.
    "ubuntu-14.04"        = "ami-78648501"
    "ubuntu-16.04"        = "ami-841ffefd"
    "ubuntu-16.10"        = "ami-8ea14ff7" # Community image.
    "ubuntu-17.04"        = "ami-9f8228e6" # Community image.
    "ubuntu-17.10"        = "ami-5815a221" # Community image.
  }
}

variable "usernames" {
  type = "map"
  default = {
    "centos-6"           = "centos"
    "centos-7"           = "centos"
    "debian-wheezy"      = "admin"
    "debian-jessie"      = "admin"
    "debian-stretch"     = "admin"
    "fedora-24"          = "fedora"
    "fedora-25"          = "fedora"
    "fedora-26"          = "fedora"
    "ubuntu-12.04"       = "ubuntu"
    "ubuntu-14.04"       = "ubuntu"
    "ubuntu-16.04"       = "ubuntu"
    "ubuntu-16.10"       = "ubuntu"
    "ubuntu-17.04"       = "ubuntu"
    "ubuntu-17.10"       = "ubuntu"
  }
}
