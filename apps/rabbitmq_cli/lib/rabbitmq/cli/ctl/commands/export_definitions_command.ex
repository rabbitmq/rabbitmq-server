## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ExportDefinitionsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

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
  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end
  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end
  # output to stdout
  def validate(["-"], _) do
    :ok
  end
  def validate([path], _) do
    dir = Path.dirname(path)
    case File.exists?(dir, [raw: true]) do
      true  -> :ok
      false -> {:validation_failure, {:bad_argument, "Directory #{dir} does not exist"}}
    end
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(["-"], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_definitions, :all_definitions, [], timeout) do
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      result -> {:ok, result}
    end
  end
  def run([path], %{node: node_name, timeout: timeout, format: format}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_definitions, :all_definitions, [], timeout) do
      {:badrpc, _} = err -> err
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      result ->
         # write to the file in run/2 because output/2 is not meant to
         # produce side effects
         body = serialise(result, format)
         abs_path = Path.absname(path)

         File.rm(abs_path)
         case File.write(abs_path, body) do
           # no output
           :ok -> {:ok, nil}
           {:error, :enoent}  ->
             {:error, ExitCodes.exit_dataerr(), "Parent directory or file #{path} does not exist"}
           {:error, :enotdir} ->
             {:error, ExitCodes.exit_dataerr(), "Parent directory of file #{path} is not a directory"}
           {:error, :enospc} ->
             {:error, ExitCodes.exit_dataerr(), "No space left on device hosting #{path}"}
           {:error, :eacces} ->
             {:error, ExitCodes.exit_dataerr(), "No permissions to write to file #{path} or its parent directory"}
           {:error, :eisdir} ->
             {:error, ExitCodes.exit_dataerr(), "Path #{path} is a directory"}
           {:error, err}     ->
             {:error, ExitCodes.exit_dataerr(), "Could not write to file #{path}: #{err}"}
         end
    end
  end

  def output({:ok, nil}, _) do
    {:ok, nil}
  end
  def output({:ok, result}, %{format: "json"}) when is_map(result) do
    {:ok, serialise(result, "json")}
  end
  def output({:ok, result}, %{format: "erlang"}) when is_map(result) do
    {:ok, serialise(result, "erlang")}
  end
  use RabbitMQ.CLI.DefaultOutput

  def printer(), do: RabbitMQ.CLI.Printers.StdIORaw

  def usage, do: "export_definitions <file_path | \"-\"> [--format <json | erlang>]"

  def usage_additional() do
    [
      ["<file>", "Local file path to export to. Pass a dash (-) for stdout."],
      ["--format", "output format to use: json or erlang"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.definitions()
    ]
  end

  def help_section(), do: :definitions

  def description(), do: "Exports definitions in JSON or compressed Erlang Term Format."

  def banner([path], %{format: fmt}), do: "Exporting definitions in #{human_friendly_format(fmt)} to a file at \"#{path}\" ..."

  #
  # Implementation
  #

  defp serialise(raw_map, "json") do
    # make sure all runtime parameter values are maps, otherwise
    # they will end up being a list of pairs (a keyword list/proplist)
    # in the resulting JSON document
    map = Map.update!(raw_map, :parameters, fn(params) ->
      Enum.map(params, fn(param) ->
        Map.update!(param, "value", &:rabbit_data_coercion.to_map/1)
      end)
    end)
    {:ok, json} = JSON.encode(map)
    json
  end

  defp serialise(map, "erlang") do
    :erlang.term_to_binary(map, [{:compressed, 9}])
  end

  defp human_friendly_format("JSON"), do: "JSON"
  defp human_friendly_format("json"), do: "JSON"
  defp human_friendly_format("erlang"), do: "Erlang term format"
end
