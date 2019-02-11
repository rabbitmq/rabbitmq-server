## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
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


defmodule RabbitMQ.CLI.Ctl.Commands.ListFeatureFlagsCommand do
  alias RabbitMQ.CLI.Ctl.InfoKeys

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  @info_keys ~w(name state stability provided_by desc doc_url)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl]
  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults([], opts), do: {["name", "state"], opts}
  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_|_] = args, %{node: node_name, timeout: time_out}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_ff_extra, :cli_info, [], time_out)
    |> filter_by_arg(args)
  end

  def usage, do: "list_feature_flags [<feature-flag-info-item> ...]"

  def usage_additional() do
    "<feature-flag-info-item> must be a member of the list [name, state, stability, provided_by, desc]."
  end

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

  def banner(_,_), do: "Listing feature flags ..."
end
