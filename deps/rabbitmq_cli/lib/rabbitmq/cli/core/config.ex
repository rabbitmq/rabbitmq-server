## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Config do
  alias RabbitMQ.CLI.{
    CommandBehaviour,
    FormatterBehaviour,
    PrinterBehaviour
  }

  alias RabbitMQ.CLI.Core.Helpers

  #
  # Environment
  #

  def get_option(name, opts \\ %{}) do
    raw_option =
      opts[name] ||
        get_system_option(name, opts) ||
        default(name)

    normalise(name, raw_option)
  end

  def output_less?(opts) do
    Map.get(opts, :silent, false) || Map.get(opts, :quiet, false)
  end

  def normalise(:node, nil), do: nil

  def normalise(:node, node) when not is_atom(node) do
    RabbitMQ.CLI.Core.DataCoercion.to_atom(node)
  end

  def normalise(:erlang_cookie, nil), do: nil

  def normalise(:erlang_cookie, c) when not is_atom(c) do
    RabbitMQ.CLI.Core.DataCoercion.to_atom(c)
  end

  def normalise(:longnames, true), do: :longnames
  def normalise(:longnames, "true"), do: :longnames
  def normalise(:longnames, 'true'), do: :longnames
  def normalise(:longnames, "\"true\""), do: :longnames
  def normalise(:longnames, _val), do: :shortnames
  def normalise(_, value), do: value

  def get_system_option(:script_name, _) do
    Path.basename(:escript.script_name())
    |> Path.rootname()
    |> String.to_atom()
  end

  def get_system_option(:aliases_file, _) do
    System.get_env("RABBITMQ_CLI_ALIASES_FILE")
  end

  def get_system_option(:erlang_cookie, _) do
    System.get_env("RABBITMQ_ERLANG_COOKIE")
  end

  def get_system_option(:node, %{offline: true} = opts) do
    remote_node =
      case opts[:node] do
        nil -> nil
        val -> Helpers.normalise_node_option(val, opts[:longnames], opts)
      end

    context = get_env_context(remote_node, true)
    get_val_from_env_context(context, :node)
  end

  def get_system_option(:node, opts) do
    remote_node =
      case opts[:node] do
        nil -> nil
        val -> Helpers.normalise_node_option(val, opts[:longnames], opts)
      end

    context = get_env_context(remote_node, false)
    get_val_from_env_context(context, :node)
  end

  def get_system_option(name, opts) do
    work_offline = opts[:offline] == true

    remote_node =
      case name do
        :longnames -> nil
        :rabbitmq_home -> nil
        _ -> node_flag_or_default(opts)
      end

    context = get_env_context(remote_node, work_offline)
    val0 = get_val_from_env_context(context, name)

    val =
      cond do
        remote_node != nil and
          val0 == :undefined and
            (name == :mnesia_dir or name == :feature_flags_file or name == :plugins_dir or
               name == :enabled_plugins_file) ->
          context1 = get_env_context(nil, true)
          get_val_from_env_context(context1, name)

        true ->
          val0
      end

    case val do
      :undefined -> nil
      _ -> val
    end
  end

  def get_env_context(nil, _) do
    :rabbit_env.get_context()
  end

  def get_env_context(remote_node, work_offline) do
    case work_offline do
      true -> :rabbit_env.get_context(:offline)
      false -> :rabbit_env.get_context(remote_node)
    end
  end

  def get_val_from_env_context(context, name) do
    case name do
      :node -> context[:nodename]
      :longnames -> context[:nodename_type] == :longnames
      :rabbitmq_home -> context[:rabbitmq_home]
      :mnesia_dir -> context[:mnesia_dir]
      :plugins_dir -> context[:plugins_path]
      :plugins_expand_dir -> context[:plugins_expand_dir]
      :feature_flags_file -> context[:feature_flags_file]
      :enabled_plugins_file -> context[:enabled_plugins_file]
    end
  end

  def node_flag_or_default(opts) do
    case opts[:node] do
      nil ->
        # Just in case `opts` was not normalized yet (to get the
        # default node), we do it here as well.
        case Helpers.normalise_node_option(opts) do
          {:error, _} -> nil
          {:ok, normalized_opts} -> normalized_opts[:node]
        end

      node ->
        node
    end
  end

  def default(:script_name), do: :rabbitmqctl
  def default(:node), do: :rabbit
  def default(_), do: nil

  #
  # Formatters and Printers
  #

  def get_formatter(command, %{formatter: formatter}) do
    module_name = FormatterBehaviour.module_name(formatter)

    case Code.ensure_loaded(module_name) do
      {:module, _} -> module_name
      {:error, :nofile} -> CommandBehaviour.formatter(command, default_formatter())
    end
  end

  def get_formatter(command, _) do
    CommandBehaviour.formatter(command, default_formatter())
  end

  def get_printer(command, %{printer: printer}) do
    module_name = PrinterBehaviour.module_name(printer)

    case Code.ensure_loaded(module_name) do
      {:module, _} -> module_name
      {:error, :nofile} -> CommandBehaviour.printer(command, default_printer())
    end
  end

  def get_printer(command, _) do
    CommandBehaviour.printer(command, default_printer())
  end

  def default_formatter() do
    RabbitMQ.CLI.Formatters.String
  end

  def default_printer() do
    RabbitMQ.CLI.Printers.StdIO
  end
end
