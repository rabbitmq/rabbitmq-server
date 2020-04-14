# vim:sw=2:et:

provider "aws" {
  region = "eu-west-1"
}

data "aws_instances" "vms" {
  instance_tags = {
    rabbitmq-testing = true
    rabbitmq-testing-id = var.uuid
  }
}

data "template_file" "erlang_node_hostname" {
  count = length(data.aws_instances.vms.ids)
  template = "$${private_dns}"
  vars = {
    // FIXME: Here we hard-code how Amazon EC2 formats hostnames based
    // on the private IP address.
    private_dns = "ip-${
      join("-", split(".", data.aws_instances.vms.private_ips[count.index]))}"
  }
}

data "template_file" "erlang_node_nodename" {
  count = length(data.aws_instances.vms.ids)
  template = "${var.erlang_nodename}@$${private_dns}"
  vars = {
    private_dns = data.template_file.erlang_node_hostname.*.rendered[count.index]
  }
}
