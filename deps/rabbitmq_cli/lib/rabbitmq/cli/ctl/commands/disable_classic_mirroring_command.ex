## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.DisableClassicMirroringCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [hard: :boolean]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", hard: false}, opts)}
  end

  def validate([_ | _] = args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_runtime_parameters_acl,
      :disallow,
      [vhost, "policy", "ha-mode", Helpers.cli_acting_user()]
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "disable_classic_mirroring [--vhost <vhost>]"

  def usage_doc_guides() do
    [
      DocGuide.mirroring(),
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :parameters

  def description(), do: "Disables HA mirroring for classic queues"

  def banner([], %{vhost: vhost}) do
    "Disabling HA mirroring for classic queues in vhost \"#{vhost}\" ..."
  end
end
