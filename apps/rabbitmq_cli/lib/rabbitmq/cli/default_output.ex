## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.DefaultOutput do
  # When `use RabbitMQ.CLI.DefaultOutput` is invoked,
  # this will define output/2 that delegates to RabbitMQ.CLI.DefaultOutput.output/2.
  defmacro __using__(_) do
    quote do
      def output(result, opts) do
        RabbitMQ.CLI.DefaultOutput.output(result, opts)
      end
    end
  end

  def output(result, opts \\ %{}) do
    format_output(normalize_output(result, opts))
  end

  def mnesia_running_error(node_name) do
    "Mnesia is still running on node #{node_name}.\n" <>
      "Please stop RabbitMQ with 'rabbitmqctl stop_app' first."
  end

  defp normalize_output(:ok, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name}}
  end
  defp normalize_output(:ok, _opts), do: :ok
  defp normalize_output({:ok, value}, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "value" => value}}
  end
  defp normalize_output({:ok, _} = input, _opts), do: input
  defp normalize_output({:stream, _} = input, _opts), do: input
  defp normalize_output({:badrpc_multi, _, _} = input, _opts), do: {:error, input}
  defp normalize_output({:badrpc, :nodedown} = input, _opts), do: {:error, input}
  defp normalize_output({:badrpc, :timeout} = input, _opts), do: {:error, input}
  defp normalize_output({:badrpc, {:timeout, _n}} = input, _opts), do: {:error, input}
  defp normalize_output({:badrpc, {:timeout, _n, _msg}} = input, _opts), do: {:error, input}
  defp normalize_output({:badrpc, {:EXIT, reason}}, _opts), do: {:error, reason}
  defp normalize_output({:error, exit_code, string}, _opts) when is_integer(exit_code) do
    {:error, exit_code, to_string(string)}
  end
  defp normalize_output({:error, format, args}, _opts)
       when (is_list(format) or is_binary(format)) and is_list(args) do
    {:error, to_string(:rabbit_misc.format(format, args))}
  end
  defp normalize_output({:error, _} = input, _opts), do: input
  defp normalize_output({:error_string, string}, _opts) do
    {:error, to_string(string)}
  end
  defp normalize_output(unknown, _opts) when is_atom(unknown), do: {:error, unknown}
  defp normalize_output({unknown, _} = input, _opts) when is_atom(unknown), do: {:error, input}
  defp normalize_output(result, _opts) when not is_atom(result), do: {:ok, result}

  
  defp format_output({:error, _} = result) do
    result
  end
  defp format_output({:error, _, _} = result) do
    result
  end

  defp format_output(:ok) do
    :ok
  end

  defp format_output({:ok, output}) do
    case Enumerable.impl_for(output) do
      nil ->
        {:ok, output}

      ## Do not streamify plain maps
      Enumerable.Map ->
        {:ok, output}

      ## Do not streamify proplists
      Enumerable.List ->
        case FormatterHelpers.proplist?(output) do
          true -> {:ok, output}
          false -> {:stream, output}
        end

      _ ->
        {:stream, output}
    end
  end

  defp format_output({:stream, stream}) do
    {:stream, stream}
  end
end
