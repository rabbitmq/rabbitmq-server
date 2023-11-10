## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2023 Broadcom. All Rights Reserved. The term “Broadcom”
## refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListDeprecatedFeaturesCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  alias RabbitMQ.CLI.Ctl.InfoKeys

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  @info_keys ~w(name deprecation_phase provided_by desc doc_url)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]

  def switches(), do: [used: :boolean]

  def merge_defaults([], opts) do
    {["name", "deprecation_phase"], Map.merge(%{used: false}, opts)}
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{used: false}, opts)}
  end

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  def validate_execution_environment(args, opts) do
    Validators.chain(
      [
        &Validators.rabbit_is_loaded/2,
        &Validators.rabbit_is_running/2
      ],
      [args, opts]
    )
  end

  def run([_ | _] = args, %{node: node_name, timeout: timeout, used: false}) do
    case :rabbit_misc.rpc_call(
           node_name,
           :rabbit_depr_ff_extra,
           :cli_info,
           [:all],
           timeout
         ) do
      # Server does not support deprecated features, consider none are available.
      {:badrpc, {:EXIT, {:undef, _}}} -> []
      {:badrpc, _} = err -> err
      val -> filter_by_arg(val, args)
    end
  end

  def run([_ | _] = args, %{node: node_name, timeout: timeout, used: true}) do
    case :rabbit_misc.rpc_call(
           node_name,
           :rabbit_deprecated_feature_extra,
           :cli_info,
           [:used],
           timeout
         ) do
      # Server does not support deprecated features, consider none are available.
      {:badrpc, {:EXIT, {:undef, _}}} -> []
      {:badrpc, _} = err -> err
      val -> filter_by_arg(val, args)
    end
  end

  def banner(_, %{used: false}), do: "Listing deprecated features ..."
  def banner(_, %{used: true}), do: "Listing deprecated features in use ..."

  def usage, do: "list_deprecated_features [--used] [<column> ...]"

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")],
      ["--used", "returns deprecated features in use"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.feature_flags()
    ]
  end

  def help_section(), do: :feature_flags

  def description(), do: "Lists deprecated features"

  #
  # Implementation
  #

  defp filter_by_arg(ff_info, _) when is_tuple(ff_info) do
    # tuple means unexpected data
    ff_info
  end

  defp filter_by_arg(ff_info, [_ | _] = args) when is_list(ff_info) do
    symbol_args = InfoKeys.prepare_info_keys(args)

    Enum.map(
      ff_info,
      fn ff ->
        symbol_args
        |> Enum.filter(fn arg -> ff[arg] != nil end)
        |> Enum.map(fn arg -> {arg, ff[arg]} end)
      end
    )
  end
end
