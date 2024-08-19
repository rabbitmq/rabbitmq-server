## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.EnableFeatureFlagCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [experimental: :boolean]
  def aliases(), do: [f: :experimental]

  def merge_defaults(args, opts), do: { args, Map.merge(%{experimental: false}, opts) }

  def validate([], _opts), do: {:validation_failure, :not_enough_args}
  def validate([_ | _] = args, _opts) when length(args) > 1, do: {:validation_failure, :too_many_args}

  def validate([""], _opts),
    do: {:validation_failure, {:bad_argument, "feature_flag cannot be an empty string."}}

  def validate([_], _opts), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(["all"], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_feature_flags, :enable_all, []) do
      # Server does not support feature flags, consider none are available.
      # See rabbitmq/rabbitmq-cli#344 for context. MK.
      {:badrpc, {:EXIT, {:undef, _}}} -> {:error, :unsupported}
      {:badrpc, _} = err -> err
      other -> other
    end
  end

  def run([feature_flag], %{node: node_name, experimental: experimental}) do
    case {experimental, :rabbit_misc.rpc_call(node_name, :rabbit_feature_flags, :get_stability, [
      String.to_atom(feature_flag)
    ])} do
          {_, {:badrpc, {:EXIT, {:undef, _}}}} -> {:error, :unsupported}
          {_, {:badrpc, _} = err} -> err
          {false, :experimental} ->
              IO.puts("Feature flag #{feature_flag} is experimental. If you understand the risk, use --experimental to enable it.")
          _ ->
                case :rabbit_misc.rpc_call(node_name, :rabbit_feature_flags, :enable, [
                  String.to_atom(feature_flag)
                ]) do
                  # Server does not support feature flags, consider none are available.
                  # See rabbitmq/rabbitmq-cli#344 for context. MK.
                  {:badrpc, {:EXIT, {:undef, _}}} -> {:error, :unsupported}
                  {:badrpc, _} = err -> err
                  other -> other
                end
    end
  end

  def output({:error, :unsupported}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(),
      "This feature flag is not supported by node #{node_name}"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "enable_feature_flag [--experimental] <all | feature_flag>"

  def usage_additional() do
    [
      [
        "<feature_flag>",
        "name of the feature flag to enable, or \"all\" to enable all supported flags"
      ],
      [
        "--experimental",
        "required to enable experimental feature flags (make sure you understand the risks!))"
      ]
    ]
  end

  def help_section(), do: :feature_flags

  def description(),
    do: "Enables a feature flag or all supported feature flags on the target node"

  def banner(["all"], _), do: "Enabling all feature flags ..."

  def banner([feature_flag], _), do: "Enabling feature flag \"#{feature_flag}\" ..."
end
