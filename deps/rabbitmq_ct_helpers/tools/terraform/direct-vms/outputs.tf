# vim:sw=2:et:

output "uuid" {
  value = local.uuid
}

output "ct_peer_ipaddrs" {
  value = zipmap(
    data.template_file.erlang_node_hostname.*.rendered,
    aws_instance.vm.*.public_ip)
}

output "ct_peer_nodenames" {
  value = zipmap(
    data.template_file.erlang_node_hostname.*.rendered,
    data.template_file.erlang_node_nodename.*.rendered)
}

output "ssh_user_and_host" {
  value = zipmap(
    data.template_file.erlang_node_hostname.*.rendered,
    formatlist("%s@%s", local.username, aws_instance.vm.*.public_dns))
}

// The following variables are used by other modules (e.g.
// `autoscaling-group`) who want to benefit from the same resources
// (beside the actual instances).

output "resource_prefix" {
  value = local.resource_prefix
}

output "instance_name" {
  value = local.vm_name
}

output "instance_ami" {
  value = local.ami
}

output "instance_type" {
  value = local.ec2_instance_type
}

output "ssh_key_name" {
  value = aws_key_pair.ci_user.key_name
}

output "security_groups" {
  value = local.security_groups
}

output "instance_user_data" {
  value = data.template_file.user_data.rendered
}

output "subnet_id" {
  value = aws_subnet.vpc.id
}
