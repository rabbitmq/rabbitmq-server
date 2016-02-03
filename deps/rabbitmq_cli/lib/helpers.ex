## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule Helpers do
  @rabbit_host "rabbit"

  def get_rabbit_hostname(), do: (@rabbit_host <> "@" <> hostname) |> String.to_atom

  def connect_to_rabbitmq(), do:      :net_kernel.connect_node(get_rabbit_hostname)
  def connect_to_rabbitmq(input), do: :net_kernel.connect_node(input)

  defp hostname(), do: :inet.gethostname() |> elem(1) |> List.to_string
end
