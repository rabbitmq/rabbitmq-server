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
## Copyright (c) 2018-2020 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.ListFeatureFlagsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  alias RabbitMQ.CLI.Ctl.InfoKeys

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  @info_keys ~w(name state stability provided_by desc doc_url)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults([], opts), do: {["name", "state"], opts}
  def merge_defaults(args, opts), do: {args, opts}

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

  def run([_|_] = args, %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_ff_extra, :cli_info, [], timeout) do
      # Server does not support feature flags, consider none are available.
      # See rabbitmq/rabbitmq-cli#344 for context. MK.
      {:badrpc, {:EXIT, {:undef, _}}} -> []
      {:badrpc, _} = err    -> err
      val                   -> filter_by_arg(val, args)
    end

  end

  def banner(_, _), do: "Listing feature flags ..."

  def usage, do: "list_feature_flags [<column> ...]"

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.feature_flags()
    ]
  end

  def help_section(), do: :feature_flags

  def description(), do: "Lists feature flags"

  #
  # Implementation
  #

  defp filter_by_arg(ff_info, _) when is_tuple(ff_info) do
    # tuple means unexpected data
    ff_info
  end

  defp filter_by_arg(ff_info, [_|_] = args) when is_list(ff_info) do
    symbol_args = InfoKeys.prepare_info_keys(args)
    Enum.map(ff_info,
      fn(ff) ->
        symbol_args
        |> Enum.filter(fn(arg) -> ff[arg] != nil end)
        |> Enum.map(fn(arg) -> {arg, ff[arg]} end)
      end
    )
  end
end
