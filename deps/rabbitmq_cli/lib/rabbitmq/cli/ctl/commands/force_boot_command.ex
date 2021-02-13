## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
