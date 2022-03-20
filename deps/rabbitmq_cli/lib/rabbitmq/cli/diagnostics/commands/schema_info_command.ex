## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.SchemaInfoCommand do
  @moduledoc """
  Lists all tables on the mnesia schema
  """

  alias RabbitMQ.CLI.Ctl.InfoKeys

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @info_keys ~w(name snmp load_order active_replicas all_nodes attributes checkpoints disc_copies
    disc_only_copies external_copies frag_properties master_nodes ram_copies
    storage_properties subscribers user_properties cstruct local_content
    where_to_commit where_to_read name access_mode cookie load_by_force
    load_node record_name size storage_type type where_to_write index arity
    majority memory commit_work where_to_wlock load_reason record_validation
   version wild_pattern index_info)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def merge_defaults([], opts) do
    merge_defaults(
      ~w(name cookie active_replicas user_properties),
      opts
    )
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  def run([_ | _] = args, %{node: node_name, timeout: timeout}) do
    info_keys = InfoKeys.prepare_info_keys(args)
    :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :schema_info, [info_keys], timeout)
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage() do
    "schema_info [--no-table-headers] [<column> ...]"
  end

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists schema database tables and their properties"

  def banner(_, %{node: node_name}), do: "Asking node #{node_name} to report its schema..."
end
