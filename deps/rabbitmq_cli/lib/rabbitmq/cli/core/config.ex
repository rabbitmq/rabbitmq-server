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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Config do

  alias RabbitMQ.CLI.{
    CommandBehaviour,
    FormatterBehaviour,
    PrinterBehaviour
  }

  #
  # Environment
  #

  def get_option(name, opts \\ %{}) do
    raw_option =
      opts[name] ||
        get_system_option(name) ||
        default(name)

    normalise(name, raw_option)
  end

  def output_less?(opts) do
    Map.get(opts, :silent, false) || Map.get(opts, :quiet, false)
  end

  def normalise(:node, nil), do: nil

  def normalise(:node, node) when not is_atom(node) do
    Rabbitmq.Atom.Coerce.to_atom(node)
  end

  def normalise(:erlang_cookie, nil), do: nil

  def normalise(:erlang_cookie, c) when not is_atom(c) do
    Rabbitmq.Atom.Coerce.to_atom(c)
  end

  def normalise(:longnames, true), do: :longnames
  def normalise(:longnames, "true"), do: :longnames
  def normalise(:longnames, 'true'), do: :longnames
  def normalise(:longnames, "\"true\""), do: :longnames
  def normalise(:longnames, _val), do: :shortnames
  def normalise(_, value), do: value

  def system_env_variable(name) do
    case name do
      :longnames -> "RABBITMQ_USE_LONGNAME"
      :rabbitmq_home -> "RABBITMQ_HOME"
      :mnesia_dir -> "RABBITMQ_MNESIA_DIR"
      :plugins_dir -> "RABBITMQ_PLUGINS_DIR"
      :plugins_expand_dir -> "RABBITMQ_PLUGINS_EXPAND_DIR"
      :feature_flags_file -> "RABBITMQ_FEATURE_FLAGS_FILE"
      :enabled_plugins_file -> "RABBITMQ_ENABLED_PLUGINS_FILE"
      :node -> "RABBITMQ_NODENAME"
      :aliases_file -> "RABBITMQ_CLI_ALIASES_FILE"
      :erlang_cookie -> "RABBITMQ_ERLANG_COOKIE"
      _ -> ""
    end
  end

  def get_system_option(:script_name) do
    Path.basename(:escript.script_name())
    |> Path.rootname()
    |> String.to_atom()
  end

  def get_system_option(name) do
    System.get_env(system_env_variable(name))
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
