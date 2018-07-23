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

# Provides common validation functions.
defmodule RabbitMQ.CLI.Core.Validators do
  alias RabbitMQ.CLI.Core.Helpers

  def chain([validator | rest], args) do
    case apply(validator, args) do
      :ok                        -> chain(rest, args)
      {:ok, _}                   -> chain(rest, args)
      {:validation_failure, err} -> {:validation_failure, err}
      {:error, err}              -> {:validation_failure, err}
    end
  end
  def chain([], _) do
    :ok
  end

  def node_is_not_running(_, %{node: node_name}) do
    case Helpers.node_running?(node_name) do
      true  -> {:validation_failure, :node_running};
      false -> :ok
    end
  end

  def node_is_running(_, %{node: node_name}) do
    case Helpers.node_running?(node_name) do
      false -> {:validation_failure, :node_not_running};
      true  -> :ok
    end
  end

  def mnesia_dir_is_set(_, opts) do
    case Helpers.require_mnesia_dir(opts) do
      :ok           -> :ok;
      {:error, err} -> {:validation_failure, err}
    end
  end

  def rabbit_is_loaded(_, opts) do
    case Helpers.require_rabbit(opts) do
      :ok           -> :ok;
      {:error, err} -> {:validation_failure, err}
    end
  end

  def rabbit_is_running(args, opts) do
    case rabbit_app_state(args, opts) do
      :running -> :ok;
      :stopped -> {:validation_failure, :rabbit_app_is_stopped};
      other    -> other
    end
  end

  def rabbit_is_running_or_offline_flag_used(_args, %{offline: true}) do
    :ok
  end
  def rabbit_is_running_or_offline_flag_used(args, opts) do
    rabbit_is_running(args, opts)
  end

  def rabbit_is_not_running(args, opts) do
    case rabbit_app_state(args, opts) do
      :running -> {:validation_failure, :rabbit_app_is_running};
      :stopped -> :ok;
      other    -> other
    end
  end

  def rabbit_app_state(_, opts) do
    case Helpers.rabbit_app_running?(opts) do
      true          -> :running;
      false         -> :stopped;
      {:error, err} -> {:error, err}
    end
  end

end
