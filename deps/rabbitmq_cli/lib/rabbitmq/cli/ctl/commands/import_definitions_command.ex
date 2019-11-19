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

defmodule RabbitMQ.CLI.Ctl.Commands.ImportDefinitionsCommand do
  alias RabbitMQ.CLI.Core.{Config, DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(["-"] = args, opts) do
    {args, Map.merge(%{format: "json", silent: true}, Helpers.case_insensitive_format(opts))}
  end
  def merge_defaults(args, opts) do
    {args, Map.merge(%{format: "json"}, Helpers.case_insensitive_format(opts))}
  end

  def switches(), do: [timeout: :integer, format: :string]
  def aliases(), do: [t: :timeout]

  def validate(_, %{format: format})
      when format != "json" and format != "JSON" and format != "erlang" do
    {:validation_failure, {:bad_argument, "Format should be either json or erlang"}}
  end
  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end
  def validate([path], _) do
    case File.exists?(path, [raw: true]) do
      true  -> :ok
      false -> {:validation_failure, {:bad_argument, "File #{path} does not exist"}}
    end
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, format: format, timeout: timeout}) do
    case IO.read(:stdio, :all) do
      :eof -> {:error, :not_enough_args}
      bin  ->
        case deserialise(bin, format) do
          {:error, error} ->
            {:error, ExitCodes.exit_dataerr(), "Failed to deserialise input (format: #{human_friendly_format(format)}) (error: #{inspect(error)})"}
          {:ok, map} ->
            :rabbit_misc.rpc_call(node_name, :rabbit_definitions, :import_parsed, [map], timeout)
        end
    end
  end
  def run([path], %{node: node_name, timeout: timeout, format: format}) do
    abs_path = Path.absname(path)

    case File.read(abs_path) do
      {:ok, ""} ->
        {:error, ExitCodes.exit_dataerr(), "File #{path} is zero-sized"}
      {:ok, bin} ->
        case deserialise(bin, format) do
          {:error, error} ->
            {:error, ExitCodes.exit_dataerr(), "Failed to deserialise input (format: #{human_friendly_format(format)}) (error: #{inspect(error)})"}
          {:ok, map} ->
            :rabbit_misc.rpc_call(node_name, :rabbit_definitions, :import_parsed, [map], timeout)
        end
      {:error, :enoent}  ->
        {:error, ExitCodes.exit_dataerr(), "Parent directory or file #{path} does not exist"}
      {:error, :enotdir} ->
        {:error, ExitCodes.exit_dataerr(), "Parent directory of file #{path} is not a directory"}
      {:error, :eacces} ->
        {:error, ExitCodes.exit_dataerr(), "No permissions to read from file #{path} or its parent directory"}
      {:error, :eisdir} ->
        {:error, ExitCodes.exit_dataerr(), "Path #{path} is a directory"}
      {:error, err}     ->
        {:error, ExitCodes.exit_dataerr(), "Could not read from file #{path}: #{err}"}
    end
  end

  def output(:ok, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name}}
  end
  def output(:ok, opts) do
    case Config.output_less?(opts) do
      true  -> :ok
      false -> {:ok, "Successfully started definition import. " <>
                     "This process is asynchronous and can take some time.\n"}
    end
  end
  use RabbitMQ.CLI.DefaultOutput

  def printer(), do: RabbitMQ.CLI.Printers.StdIORaw

  def usage, do: "import_definitions <file_path | \"-\"> [--format <json | erlang>]"

  def usage_additional() do
    [
      ["[file]", "Local file path to import from. If omitted will be read from standard input."],
      ["--format", "input format to use: json or erlang"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.definitions()
    ]
  end

  def help_section(), do: :definitions

  def description(), do: "Imports definitions in JSON or compressed Erlang Term Format."

  def banner([], %{format: fmt}) do
    "Importing definitions in #{human_friendly_format(fmt)} from standard input ..."
  end
  def banner([path], %{format: fmt}) do
    "Importing definitions in #{human_friendly_format(fmt)} from a file at \"#{path}\" ..."
  end

  #
  # Implementation
  #

  defp deserialise(bin, "json") do
    JSON.decode(bin)
  end

  defp deserialise(bin, "erlang") do
    try do
      {:ok, :erlang.binary_to_term(bin)}
    rescue e in ArgumentError ->
      {:error, e.message}
    end
  end

  defp human_friendly_format("JSON"), do: "JSON"
  defp human_friendly_format("json"), do: "JSON"
  defp human_friendly_format("erlang"), do: "Erlang term format"
end
