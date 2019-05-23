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
alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.DefaultOutput do
  # When `use RabbitMQ.CLI.DefaultOutput` is invoked,
  # this will define output/2 that delegates to RabbitMQ.CLI.DefaultOutput.output/3.
  defmacro __using__(_) do
    quote do
      def output(result, _opts) do
        RabbitMQ.CLI.DefaultOutput.output(result)
      end
    end
  end

  def output(result) do
    result
    |> normalize_output()
    |> format_output()
  end

  def mnesia_running_error(node_name) do
    "Mnesia is still running on node #{node_name}.\n" <>
      "Please stop RabbitMQ with 'rabbitmqctl stop_app' first."
  end

  defp normalize_output(:ok), do: :ok
  defp normalize_output({:ok, _} = input), do: input
  defp normalize_output({:stream, _} = input), do: input
  defp normalize_output({:badrpc_multi, _, _} = input), do: {:error, input}
  defp normalize_output({:badrpc, :nodedown} = input), do: {:error, input}
  defp normalize_output({:badrpc, :timeout} = input), do: {:error, input}
  defp normalize_output({:badrpc, {:timeout, _n}} = input), do: {:error, input}
  defp normalize_output({:badrpc, {:timeout, _n, _msg}} = input), do: {:error, input}
  defp normalize_output({:badrpc, {:EXIT, reason}}), do: {:error, reason}
  defp normalize_output({:error, format, args})
       when (is_list(format) or is_binary(format)) and is_list(args) do
    {:error, to_string(:rabbit_misc.format(format, args))}
  end

  defp normalize_output({:error_string, string}) do
    {:error, to_string(string)}
  end

  defp normalize_output({:error, _} = input), do: input
  defp normalize_output(unknown) when is_atom(unknown), do: {:error, unknown}
  defp normalize_output({unknown, _} = input) when is_atom(unknown), do: {:error, input}
  defp normalize_output(result) when not is_atom(result), do: {:ok, result}

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
