## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
