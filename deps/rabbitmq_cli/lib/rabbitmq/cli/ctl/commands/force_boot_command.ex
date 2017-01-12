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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

alias RabbitMQ.CLI.Core.Config, as: Config

defmodule RabbitMQ.CLI.Ctl.Commands.ForceBootCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end

  def validate([], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit, :is_running, []) do
      true -> {:validation_failure, :rabbit_running};
      _    -> :ok
    end
  end

  def run([], %{node: node_name} = opts) do
    case :rabbit_misc.rpc_call(node_name,
                               :rabbit_mnesia, :force_load_next_boot, []) do
      {:badrpc, :nodedown} ->
        case Config.get_option(:mnesia_dir, opts) do
          nil        ->
            {:error, :mnesia_dir_not_found};
          dir ->
            File.write(Path.join(dir, "force_load"), "")
        end;
      _ -> :ok
    end
  end

  def usage, do: "force_boot"

  def banner(_, _), do: nil

end
