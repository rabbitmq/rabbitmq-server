## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Memory do
  alias RabbitMQ.CLI.InformationUnit, as: IU

  def memory_units do
    ["k", "kiB", "M", "MiB", "G", "GiB", "kB", "MB", "GB", ""]
  end

  def memory_unit_absolute(num, unit) when is_number(num) and num < 0,
    do: {:bad_argument, [num, unit]}

  def memory_unit_absolute(num, "k") when is_number(num), do: power_as_int(num, 2, 10)
  def memory_unit_absolute(num, "kiB") when is_number(num), do: power_as_int(num, 2, 10)
  def memory_unit_absolute(num, "M") when is_number(num), do: power_as_int(num, 2, 20)
  def memory_unit_absolute(num, "MiB") when is_number(num), do: power_as_int(num, 2, 20)
  def memory_unit_absolute(num, "G") when is_number(num), do: power_as_int(num, 2, 30)
  def memory_unit_absolute(num, "GiB") when is_number(num), do: power_as_int(num, 2, 30)
  def memory_unit_absolute(num, "kB") when is_number(num), do: power_as_int(num, 10, 3)
  def memory_unit_absolute(num, "MB") when is_number(num), do: power_as_int(num, 10, 6)
  def memory_unit_absolute(num, "GB") when is_number(num), do: power_as_int(num, 10, 9)
  def memory_unit_absolute(num, "") when is_number(num), do: num
  def memory_unit_absolute(num, unit) when is_number(num), do: {:bad_argument, [unit]}
  def memory_unit_absolute(num, unit), do: {:bad_argument, [num, unit]}

  def power_as_int(num, x, y), do: round(num * :math.pow(x, y))

  def compute_relative_values(all_pairs) when is_map(all_pairs) do
    compute_relative_values(Enum.into(all_pairs, []))
  end
  def compute_relative_values(all_pairs) do
    num_pairs = Keyword.delete(all_pairs, :strategy)
    # Includes RSS, allocated and runtime-used ("erlang") values.
    # See https://github.com/rabbitmq/rabbitmq-server/pull/1404.
    totals = Keyword.get(num_pairs, :total)
    pairs = Keyword.delete(num_pairs, :total)
    # Should not be necessary but be more defensive.
    total =
      max_of(totals) ||
        Keyword.get(totals, :rss) ||
        Keyword.get(totals, :allocated) ||
        Keyword.get(totals, :erlang)

    pairs
    |> Enum.map(fn {k, v} ->
      pg = (v / total) |> fraction_to_percent()
      {k, %{bytes: v, percentage: pg}}
    end)
    |> Enum.sort_by(fn {_key, %{bytes: bytes}} -> bytes end, &>=/2)
  end

  def formatted_watermark(val) when is_float(val) do
    %{relative: val}
  end
  def formatted_watermark({:relative, val}) when is_float(val) do
    %{relative: val}
  end
  def formatted_watermark(:infinity) do
    %{relative: 1.0}
  end
  def formatted_watermark({:absolute, val}) do
    %{absolute: parse_watermark(val)}
  end
  def formatted_watermark(val) when is_integer(val) do
    %{absolute: parse_watermark(val)}
  end
  def formatted_watermark(val) when is_bitstring(val) do
    %{absolute: parse_watermark(val)}
  end
  def formatted_watermark(val) when is_list(val) do
    %{absolute: parse_watermark(val)}
  end

  def parse_watermark({:absolute, n}) do
    case IU.parse(n) do
      {:ok, parsed} -> parsed
      err           -> err
    end
  end
  def parse_watermark(n) when is_bitstring(n) do
    case IU.parse(n) do
      {:ok, parsed} -> parsed
      err           -> err
    end
  end
  def parse_watermark(n) when is_list(n) do
    case IU.parse(n) do
      {:ok, parsed} -> parsed
      err           -> err
    end
  end
  def parse_watermark(n) when is_float(n) or is_integer(n) do
    n
  end

  #
  # Implementation
  #

  defp fraction_to_percent(x) do
    Float.round(x * 100, 2)
  end

  defp max_of(m) do
    Keyword.values(m) |> Enum.max()
  end
end
