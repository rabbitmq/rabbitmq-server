## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearParameterCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate(args, _) when is_list(args) and length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate([_ | _] = args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([component_name, key], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_runtime_parameters,
      :clear,
      [vhost, component_name, key, Helpers.cli_acting_user()]
    )
  end

  def usage, do: "clear_parameter [--vhost <vhost>] <component_name> <name>"

  def usage_additional() do
    [
      ["<component_name>", "component name"],
      ["<name>", "parameter name (identifier)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :parameters

  def description(), do: "Clears a runtime parameter."

  def banner([component_name, key], %{vhost: vhost}) do
    "Clearing runtime parameter \"#{key}\" for component \"#{component_name}\" on vhost \"#{vhost}\" ..."
  end
end
