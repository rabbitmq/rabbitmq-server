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


ExUnit.start()

defmodule TestHelper do
  def get_rabbit_hostname() do
   "rabbit@" <> hostname() |> String.to_atom()
  end

  def hostname() do
    elem(:inet.gethostname,1) |> List.to_string()
  end

  def add_vhost(name) do
    :rabbit_misc.rpc_call(get_rabbit_hostname, :rabbit_vhost, :add, [name])
  end

  def delete_vhost(name) do
    :rabbit_misc.rpc_call(get_rabbit_hostname, :rabbit_vhost, :delete, [name])
  end

  def trace_on(vhost) do
    :rabbit_misc.rpc_call(:rabbit_trace, :rabbit_trace, :start, [vhost])
  end

  def trace_off(vhost) do
    :rabbit_misc.rpc_call(:rabbit_trace, :rabbit_trace, :stop, [vhost])
  end
end
