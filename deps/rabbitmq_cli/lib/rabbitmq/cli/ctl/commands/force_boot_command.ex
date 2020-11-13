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

defmodule RabbitMQ.CLI.Ctl.Commands.ForceBootCommand do
  alias RabbitMQ.CLI.Core.{Config, DocGuide}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  ##
  def validate_execution_environment(args, opts) do
    ## We don't use RequiresRabbitAppStopped helper because we don't want to fail
    ## the validation if the node is not running.
    case RabbitMQ.CLI.Core.Validators.rabbit_is_not_running(args, opts) do
      :ok -> :ok
      {:validation_failure, _} = failure -> failure
      _other -> RabbitMQ.CLI.Core.Validators.node_is_not_running(args, opts)
    end
  end

  def run([], %{node: node_name} = opts) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :force_load_next_boot, []) do
      {:badrpc, :nodedown} ->
        case Config.get_option(:mnesia_dir, opts) do
          nil ->
            {:error, :mnesia_dir_not_found}

          dir ->
            File.write(Path.join(dir, "force_load"), "")
        end

      _ ->
        :ok
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "force_boot"

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Forces node to start even if it cannot contact or rejoin any of its previously known peers"

  def banner(_, _), do: nil
end
