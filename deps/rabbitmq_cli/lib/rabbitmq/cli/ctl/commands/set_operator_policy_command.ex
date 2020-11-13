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

defmodule RabbitMQ.CLI.Ctl.Commands.SetOperatorPolicyCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [priority: :integer, apply_to: :string]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", priority: 0, apply_to: "all"}, opts)}
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

  def run([name, pattern, definition], %{
        node: node_name,
        vhost: vhost,
        priority: priority,
        apply_to: apply_to
      }) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_policy,
      :parse_set_op,
      [vhost, name, pattern, definition, priority, apply_to, Helpers.cli_acting_user()]
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "set_operator_policy [--vhost <vhost>] [--priority <priority>] [--apply-to <apply-to>] <name> <pattern> <definition>"
  end

  def usage_additional() do
    [
      ["<name>", "policy name (identifier)"],
      ["<pattern>", "a regular expression pattern that will be used to match queue, exchanges, etc"],
      ["<definition>", "policy definition (arguments). Must be a valid JSON document"],
      ["--priority <priority>", "policy priority"],
      ["--apply-to <queues | exchanges | all>", "policy should only apply to queues, exchanges, or all entities (both of the above)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :policies

  def description(), do: "Sets an operator policy that overrides a subset of arguments in user policies"

  def banner([name, pattern, definition], %{vhost: vhost, priority: priority}) do
    "Setting operator policy override \"#{name}\" for pattern \"#{pattern}\" to \"#{definition}\" with priority \"#{
      priority
    }\" for vhost \"#{vhost}\" ..."
  end
end
