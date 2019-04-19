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
## Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.EnableFeatureFlagCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, opts}

  def validate([], _), do: {:validation_failure, :not_enough_args}
  def validate([_|_] = args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate([""], _), do: {:validation_failure, {:bad_argument, "feature_flag cannot be an empty string."}}
  def validate([_], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([feature_flag], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_feature_flags, :enable, [String.to_atom(feature_flag)]) do
      # Server does not support feature flags, consider none are available.
      # See rabbitmq/rabbitmq-cli#344 for context. MK.
      {:badrpc, {:EXIT, {:undef, _}}} -> {:error, :unsupported}
      {:badrpc, _} = err    -> err
      other                 -> other
    end
  end

  def output({:error, :unsupported}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "This feature flag is not supported by node #{node_name}"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "enable_feature_flag <feature_flag>"

  def help_section(), do: :feature_flags

  def description(), do: "Enables a feature flag on target node"

  def banner([feature_flag], _), do: "Enabling feature flag \"#{feature_flag}\" ..."
end
