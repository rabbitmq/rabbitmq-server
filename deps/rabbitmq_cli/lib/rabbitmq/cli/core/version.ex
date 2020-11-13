## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Version do
  @default_timeout 30_000

  def local_version do
    to_string(:rabbit_misc.version())
  end


  def remote_version(node_name) do
    remote_version(node_name, @default_timeout)
  end
  def remote_version(node_name, timeout) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_misc, :version, [], timeout) do
      {:badrpc, _} = err -> err
      val                -> val
    end
  end
end
