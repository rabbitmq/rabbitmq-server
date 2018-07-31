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


defmodule RabbitMQ.CLI.Diagnostics.Commands.MemoryBreakdownCommand do
  alias RabbitMQ.CLI.InformationUnit, as: IU

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [unit: :string, timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{unit: "gb"}, opts)}
  end

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, %{unit: unit}) do
    case IU.known_unit?(unit) do
      true ->
        :ok
      false ->
        {:validation_failure, "unit '#{unit}' is not supported. Please use one of: bytes, mb, gb"}
    end
  end

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vm, :memory, [], timeout)
  end

  def output(result, _options) do
    {:ok, compute_relative_values(result)}
  end

  def usage, do: "memory_breakdown [--unit <unit>]"

  def banner([], %{node: node_name}) do
    "Reporting memory breakdown on node #{node_name}..."
  end

  defmodule Formatter do
    alias RabbitMQ.CLI.Formatters.FormatterHelpers
    alias RabbitMQ.CLI.InformationUnit, as: IU

    @behaviour RabbitMQ.CLI.FormatterBehaviour

    def format_output(output, %{unit: unit}) do
      Enum.reduce(output, "", fn({key, %{bytes: bytes, percentage: percentage}}, acc) ->
        u = String.downcase(unit)
        acc <> "#{key}: #{IU.convert(bytes, u)} #{u} (#{percentage}%)\n"
      end)
    end

    def format_stream(stream, options) do
      Stream.map(stream,
        FormatterHelpers.without_errors_1(
          fn(el) ->
            format_output(el, options)
          end))
    end
  end

  def formatter(), do: Formatter


  #
  # Implementation
  #

  defp compute_relative_values(all_pairs) do
    num_pairs = Keyword.delete(all_pairs, :strategy)
    # Includes RSS, allocated and runtime-used ("erlang") values.
    # See https://github.com/rabbitmq/rabbitmq-server/pull/1404.
    totals    = Keyword.get(num_pairs, :total)
    pairs     = Keyword.delete(num_pairs, :total)
    total     = max_of(totals) ||
                # Should not be necessary but be more defensive.
                Keyword.get(totals, :rss) ||
                Keyword.get(totals, :allocated) ||
                Keyword.get(totals, :erlang)

    pairs
    |> Enum.map(fn({k, v}) ->
      pg = (v / total) |> fraction_to_percent()
      {k, %{bytes: v, percentage: pg}}
    end)
    |> Enum.sort_by(fn({_key, %{bytes: bytes}}) -> bytes end, &>=/2)
  end

  defp fraction_to_percent(x) do
    Float.round(x * 100, 2)
  end

  defp max_of(m) do
    Keyword.values(m) |> Enum.max
  end
end
