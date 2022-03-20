## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetParameterCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate([_ | _] = args, _) when length(args) < 3 do
    {:validation_failure, :not_enough_args}
  end

  def validate([_ | _] = args, _) when length(args) > 3 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([component_name, name, value], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_runtime_parameters,
      :parse_set,
      [vhost, component_name, name, value, Helpers.cli_acting_user()]
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "set_parameter [--vhost <vhost>] <component_name> <name> <value>"

  def usage_additional() do
    [
      ["<component_name>", "component name"],
      ["<name>", "parameter name (identifier)"],
      ["<value>", "parameter value"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :parameters

  def description(), do: "Sets a runtime parameter."

  def banner([component_name, name, value], %{vhost: vhost}) do
    "Setting runtime parameter \"#{name}\" for component \"#{component_name}\" to \"#{value}\" in vhost \"#{
      vhost
    }\" ..."
  end
end
