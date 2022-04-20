# vim:sw=2:et:

output "instance_count" {
  value = length(data.aws_instances.vms.ids)
}

output "instance_ids" {
  value = data.aws_instances.vms.ids
}

output "ct_peer_ipaddrs" {
  value = zipmap(
    data.template_file.erlang_node_hostname.*.rendered,
    data.aws_instances.vms.public_ips)
}

output "ct_peer_nodenames" {
  value = zipmap(
    data.template_file.erlang_node_hostname.*.rendered,
    data.template_file.erlang_node_nodename.*.rendered)
}
