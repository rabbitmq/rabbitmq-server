# vim:sw=2:et:

provider "aws" {
  region = "eu-west-1"
}

module "direct_vms" {
  source = "../direct-vms"

  instance_count = 0

  erlang_version = var.erlang_version
  erlang_git_ref = var.erlang_git_ref
  elixir_version = var.elixir_version
  erlang_cookie = var.erlang_cookie
  erlang_nodename = var.erlang_nodename
  ssh_key = var.ssh_key
  upload_dirs_archive = var.upload_dirs_archive
  instance_name_prefix = var.instance_name_prefix
  instance_name = var.instance_name
  instance_name_suffix = var.instance_name_suffix
  vpc_cidr_block = var.vpc_cidr_block
  files_suffix = var.files_suffix
  aws_ec2_region = var.aws_ec2_region
}

resource "aws_launch_configuration" "lc" {
  name_prefix     = module.direct_vms.resource_prefix

  image_id        = module.direct_vms.instance_ami
  instance_type   = module.direct_vms.instance_type
  key_name        = module.direct_vms.ssh_key_name

  security_groups = module.direct_vms.security_groups

  user_data       = module.direct_vms.instance_user_data

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_autoscaling_group" "asg" {
  name_prefix          = module.direct_vms.resource_prefix
  launch_configuration = aws_launch_configuration.lc.name
  min_size             = var.instance_count
  max_size             = var.instance_count
  desired_capacity     = var.instance_count

  vpc_zone_identifier  = [module.direct_vms.subnet_id]

  tags = [
    {
      key                 = "Name"
      value               = "${module.direct_vms.instance_name} (ASG)"
      propagate_at_launch = true
    },
    {
      key                 = "rabbitmq-testing"
      value               = true
      propagate_at_launch = true
    },
    {
      key                 = "rabbitmq-testing-id"
      value               = module.direct_vms.uuid
      propagate_at_launch = true
    },
    {
      key                 = "rabbitmq-testing-suffix"
      value               = var.files_suffix
      propagate_at_launch = true
    }
  ]

  lifecycle {
    create_before_destroy = true
  }
}
