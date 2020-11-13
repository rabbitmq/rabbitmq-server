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

defmodule RabbitMQ.CLI.Diagnostics.Commands.MemoryBreakdownCommand do
  alias RabbitMQ.CLI.InformationUnit, as: IU
  import RabbitMQ.CLI.Core.Memory

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

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vm, :memory, [], timeout)
  end

  def output(result, %{formatter: "json"} = _opts) do
    {:ok, compute_relative_values(result)}
  end

  def output(result, %{formatter: "csv"} = _opts) do
    flattened =
      compute_relative_values(result)
      |> Enum.flat_map(fn {k, %{bytes: b, percentage: p}} ->
        [{"#{k}.bytes", b}, {"#{k}.percentage", p}]
      end)
      |> Enum.sort_by(fn {key, _val} -> key end, &>=/2)

    headers = Enum.map(flattened, fn {k, _v} -> k end)
    values = Enum.map(flattened, fn {_k, v} -> v end)

    {:stream, [headers, values]}
  end

  def output(result, _opts) do
    {:ok, compute_relative_values(result)}
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Provides a memory usage breakdown on the target node."

  def usage, do: "memory_breakdown [--unit <unit>]"

  def usage_additional() do
    [
      ["--unit <bytes | mb | gb>", "byte multiple (bytes, megabytes, gigabytes) to use"],
      ["--formatter <json | csv>", "alternative formatter to use, JSON or CSV"]
    ]
  end

  def banner([], %{node: node_name}) do
    "Reporting memory breakdown on node #{node_name}..."
  end

  defmodule Formatter do
    alias RabbitMQ.CLI.Formatters.FormatterHelpers
    alias RabbitMQ.CLI.InformationUnit, as: IU

    @behaviour RabbitMQ.CLI.FormatterBehaviour

    def format_output(output, %{unit: unit}) do
      Enum.reduce(output, "", fn {key, %{bytes: bytes, percentage: percentage}}, acc ->
        u = String.downcase(unit)
        acc <> "#{key}: #{IU.convert(bytes, u)} #{u} (#{percentage}%)\n"
      end)
    end

    def format_stream(stream, options) do
      Stream.map(
        stream,
        FormatterHelpers.without_errors_1(fn el ->
          format_output(el, options)
        end)
      )
    end
  end

  def formatter(), do: Formatter
end
