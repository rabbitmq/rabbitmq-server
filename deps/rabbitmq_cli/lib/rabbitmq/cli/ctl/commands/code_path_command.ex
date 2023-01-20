## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.CodePathCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def run(["add"|dirs], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name,
      :code,
      :add_paths,
      [Enum.map(dirs, fn e -> to_charlist(e) end)])
  end

  def run(["delete", dir], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :code, :del_path, [to_charlist(dir)]) do
      :true -> :ok;
      :false -> {:false, dir};
      {:error, what} = error -> error
    end
  end

  def validate([], _options) do
    {:validation_failure, :not_enough_args}
  end

  def validate([_], _options) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["add"|dirs], _options) when length(dirs) >= 1 do
    :ok
  end

  def validate(["delete", _dir], _options) do
    :ok
  end

  def validate(["delete" | _dirs], _options) do
    {
      :validation_failure,
      {:bad_argument, "'delete' selector only takes one directory argument"}
    }
  end

  def validate([_|_], _options) do
    {
      :validation_failure,
      {:bad_argument, "Selector needs to be either 'add' or 'delete'."}
    }
  end

  def output({:false, dir}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Dir #{dir} not found in current path"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add <directory_path list> | delete <directory_path>"

  def banner(["add" | args], %{node: node_name}),
    do: "Will try to add code path(s) #{inspect(args)} on node #{node_name}"

  def banner(["delete", arg], %{node: node_name}),
    do: "Will delete code path #{inspect(arg)} on node #{node_name}"
end
