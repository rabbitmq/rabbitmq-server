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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.DefaultOutput do
  alias RabbitMQ.CLI.ExitCodes, as: ExitCodes

  # When `use RabbitMQ.CLI.DefaultOutput` is invoked,
  # this will define output/2 that delegates to RabbitMQ.CLI.DefaultOutput.output/2.
  defmacro __using__(_) do
    quote do
      def output(result, opts) do
        RabbitMQ.CLI.DefaultOutput.output(result, opts)
      end
    end
  end

  def output(result, opts) do
    result
    |> normalize_output()
    |> format_output(opts)
  end

  def mnesia_running_error(node_name) do
    "Mnesia is still running on node #{node_name}.\n" <>
    "Please stop RabbitMQ with rabbitmqctl stop_app first."
  end

  defp normalize_output(:ok), do: :ok
  defp normalize_output({:ok, _} = input), do: input
  defp normalize_output({:badrpc, :nodedown} = input), do: input
  defp normalize_output({:badrpc, :timeout} = input), do: input
  defp normalize_output({:error, _} = input), do: input
  defp normalize_output({:error_string, _} = input), do: input
  defp normalize_output(unknown) when is_atom(unknown), do: {:error, unknown}
  defp normalize_output({unknown, _} = input) when is_atom(unknown), do: {:error, input}
  defp normalize_output(result) when not is_atom(result), do: {:ok, result}

  defp format_output({:badrpc, :nodedown} = result, opts) do
    {:error, ExitCodes.exit_code_for(result),
     "Error: unable to connect to node '#{opts[:node]}': nodedown"}
  end
  defp format_output({:badrpc, :timeout} = result, opts) do
    {:error, ExitCodes.exit_code_for(result),
     "Error: {timeout, #{opts[:timeout]}}"}
  end
  defp format_output({:error, err} = result, _) do
    string_err = string_or_inspect(err)
    {:error, ExitCodes.exit_code_for(result), "Error:\n#{string_err}"}
  end
  defp format_output({:error_string, error_string}, _) do
    {:error, ExitCodes.exit_software, error_string}
  end
  defp format_output(:ok, _) do
    :ok
  end
  defp format_output({:ok, output}, _) do
    case Enumerable.impl_for(output) do
      nil -> {:ok, output};
      _   -> {:stream, output}
    end
  end

  defp string_or_inspect(val) do
    case String.Chars.impl_for(val) do
      nil -> inspect(val);
      _   -> to_string(val)
    end
  end
end
