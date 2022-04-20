# vim:sw=2:et:

provider "aws" {
  region = var.aws_ec2_region
}

locals {
  vm_name           = "${var.instance_name_prefix}${var.instance_name}${var.instance_name_suffix} - Erlang ${var.erlang_version}"

  resource_prefix   = "rabbitmq-testing-"
  distribution      = lookup(var.erlang_version_to_system, var.erlang_version)
  ec2_instance_type = lookup(var.ec2_instance_types, local.distribution, "m5.large")
  ami               = lookup(var.amis, local.distribution)
  username          = lookup(var.usernames, local.distribution, "ec2-user")
}

// The directories archive is uploaded to Amazon S3. We first create a
// temporary bucket.
//
// Note that we use this unique bucket name as a unique ID elsewhere.
resource "aws_s3_bucket" "dirs_archive" {
  bucket_prefix = local.resource_prefix
  acl           = "private"
}

locals {
  uuid             = replace(aws_s3_bucket.dirs_archive.id, local.resource_prefix, "")
  dirs_archive     = var.upload_dirs_archive
}

// We configure a VPC and a bucket policy to allow us to make the
// directories archive private on S3 but still access it from the VMs.
resource "aws_vpc" "vpc" {
  cidr_block = var.vpc_cidr_block

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name                    = local.vm_name
    rabbitmq-testing        = true
    rabbitmq-testing-id     = local.uuid
    rabbitmq-testing-suffix = var.files_suffix
  }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.vpc.id
}

resource "aws_default_route_table" "rt" {
  default_route_table_id = aws_vpc.vpc.default_route_table_id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_vpc_endpoint" "vpc" {
  vpc_id          = aws_vpc.vpc.id
  service_name    = "com.amazonaws.${aws_s3_bucket.dirs_archive.region}.s3"
  route_table_ids = [aws_default_route_table.rt.id]
}

resource "aws_subnet" "vpc" {
  cidr_block              = var.vpc_cidr_block
  vpc_id                  = aws_vpc.vpc.id
  map_public_ip_on_launch = true
}

resource "aws_s3_bucket_policy" "dirs_archive" {
  bucket = aws_s3_bucket.dirs_archive.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Id": "Policy",
  "Statement": [
    {
      "Sid": "Access-to-specific-VPCE-only",
      "Action": "s3:*",
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::${aws_s3_bucket.dirs_archive.id}",
                   "arn:aws:s3:::${aws_s3_bucket.dirs_archive.id}/*"],
      "Condition": {
        "StringEquals": {
          "aws:sourceVpce": "${aws_vpc_endpoint.vpc.id}"
        }
      },
      "Principal": "*"
    }
  ]
}
EOF
}

// We are now ready to actually upload the directories archive to S3.
resource "aws_s3_bucket_object" "dirs_archive" {
  bucket = aws_s3_bucket.dirs_archive.id
  key    = basename(local.dirs_archive)
  source = local.dirs_archive
}

// SSH key to communicate with the VMs.
resource "aws_key_pair" "ci_user" {
  key_name_prefix = local.resource_prefix
  public_key      = file("${var.ssh_key}.pub")
}

// Security group to allow SSH connections.
resource "aws_security_group" "allow_ssh" {
  name_prefix = "${local.resource_prefix}ssh-"
  description = "Allow incoming SSH connections"
  vpc_id      = aws_vpc.vpc.id

  ingress {
    from_port   = 0
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

// We need a security group to allow Erlang distribution between VMs and
// also with the local host.
resource "aws_security_group" "allow_erlang_dist" {
  name_prefix = "${local.resource_prefix}erlang-"
  description = "Allow Erlang distribution connections"
  vpc_id      = aws_vpc.vpc.id

  ingress {
    from_port   = 0
    to_port     = 4369
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 10240
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

// Setup script executed on VMs on startup. Its main purpose is to
// install and configure Erlang, and start an Erlang node to later
// control the VM.
data "template_file" "user_data" {
  template = file("${path.module}/templates/setup-erlang.sh")
  vars = {
    default_user     = local.username
    distribution     = local.distribution

    dirs_archive_url = "http://${aws_s3_bucket.dirs_archive.bucket_domain_name}/${aws_s3_bucket_object.dirs_archive.id}"
    erlang_cookie    = var.erlang_cookie
    erlang_nodename  = var.erlang_nodename
    erlang_version   = var.erlang_version
    erlang_git_ref   = var.erlang_git_ref
    elixir_version   = var.elixir_version
  }
}

locals {
  security_groups = [
    aws_security_group.allow_ssh.id,
    aws_security_group.allow_erlang_dist.id,
  ]
}

// With the directories archive and the VPC in place, we can spawn the
// VMs.
resource "aws_instance" "vm" {
  ami             = local.ami
  instance_type   = local.ec2_instance_type
  count           = var.instance_count
  key_name        = aws_key_pair.ci_user.key_name

  subnet_id       = aws_subnet.vpc.id

  vpc_security_group_ids = local.security_groups

  user_data = data.template_file.user_data.rendered

  // We need about 1.5 GiB of storage space, but apparently, 8 GiB is
  // the minimum.
  root_block_device {
    volume_size           = 8
    delete_on_termination = true
  }

  tags = {
    Name                = "${local.vm_name} - #${count.index}"
    rabbitmq-testing    = true
    rabbitmq-testing-id = local.uuid
  }

  connection {
    type        = "ssh"
    user        = local.username
    private_key = file(var.ssh_key)
    agent       = false
  }
}

data "template_file" "erlang_node_hostname" {
  count = var.instance_count
  template = "$${private_dns}"
  vars = {
    private_dns = element(split(".", aws_instance.vm.*.private_dns[count.index]), 0)
  }
}

data "template_file" "erlang_node_nodename" {
  count = var.instance_count
  template = "${var.erlang_nodename}@$${private_dns}"
  vars = {
    private_dns = data.template_file.erlang_node_hostname.*.rendered[count.index]
  }
}
