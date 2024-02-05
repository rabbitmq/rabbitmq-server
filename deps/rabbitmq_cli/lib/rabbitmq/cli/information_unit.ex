## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.InformationUnit do
  require MapSet

  @kilobyte_bytes 1000
  @megabyte_bytes @kilobyte_bytes * 1000
  @gigabyte_bytes @megabyte_bytes * 1000
  @terabyte_bytes @gigabyte_bytes * 1000
  @petabyte_bytes @terabyte_bytes * 1000

  @kibibyte_bytes 1024
  @mebibyte_bytes @kibibyte_bytes * 1024
  @gibibyte_bytes @mebibyte_bytes * 1024
  @tebibyte_bytes @gibibyte_bytes * 1024
  @pebibyte_bytes @tebibyte_bytes * 1024

  def known_units() do
    MapSet.new([
      "bytes",
      "k",
      "kb",
      "ki",
      "kib",
      "kilobytes",
      "m",
      "mb",
      "mi",
      "mib",
      "megabytes",
      "g",
      "gb",
      "gi",
      "gib",
      "gigabytes",
      "t",
      "tb",
      "ti",
      "tib",
      "terabytes",
      "p",
      "pb",
      "pi",
      "pib",
      "petabytes"
    ])
  end

  def parse(val) do
    :rabbit_resource_monitor_misc.parse_information_unit(val)
  end

  def convert(bytes, "bytes") when is_number(bytes) do
    bytes
  end

  def convert(bytes, unit) when is_number(bytes) do
    do_convert(bytes, String.downcase(unit))
  end

  def convert(:unknown, _) do
    :unknown
  end

  def known_unit?(val) do
    MapSet.member?(known_units(), String.downcase(val))
  end

  defp do_convert(bytes, "kb") do
    Float.round(bytes / @kilobyte_bytes, 4)
  end

  defp do_convert(bytes, "k"), do: do_convert(bytes, "kb")

  defp do_convert(bytes, "ki") do
    Float.round(bytes / @kibibyte_bytes, 4)
  end

  defp do_convert(bytes, "kib"), do: do_convert(bytes, "ki")
  defp do_convert(bytes, "kilobytes"), do: do_convert(bytes, "kb")

  defp do_convert(bytes, "mb") do
    Float.round(bytes / @megabyte_bytes, 4)
  end

  defp do_convert(bytes, "m"), do: do_convert(bytes, "mb")

  defp do_convert(bytes, "mi") do
    Float.round(bytes / @mebibyte_bytes, 4)
  end

  defp do_convert(bytes, "mib"), do: do_convert(bytes, "mi")
  defp do_convert(bytes, "megabytes"), do: do_convert(bytes, "mb")

  defp do_convert(bytes, "gb") do
    Float.round(bytes / @gigabyte_bytes, 4)
  end

  defp do_convert(bytes, "g"), do: do_convert(bytes, "gb")

  defp do_convert(bytes, "gi") do
    Float.round(bytes / @gigabyte_bytes, 4)
  end

  defp do_convert(bytes, "gib"), do: do_convert(bytes, "gi")
  defp do_convert(bytes, "gigabytes"), do: do_convert(bytes, "gb")

  defp do_convert(bytes, "tb") do
    Float.round(bytes / @terabyte_bytes, 4)
  end

  defp do_convert(bytes, "t"), do: do_convert(bytes, "tb")

  defp do_convert(bytes, "ti") do
    Float.round(bytes / @tebibyte_bytes, 4)
  end

  defp do_convert(bytes, "tib"), do: do_convert(bytes, "ti")
  defp do_convert(bytes, "terabytes"), do: do_convert(bytes, "tb")

  defp do_convert(bytes, "pb") do
    Float.round(bytes / @petabyte_bytes, 4)
  end

  defp do_convert(bytes, "p"), do: do_convert(bytes, "pb")

  defp do_convert(bytes, "pi") do
    Float.round(bytes / @pebibyte_bytes, 4)
  end

  defp do_convert(bytes, "pib"), do: do_convert(bytes, "pi")
  defp do_convert(bytes, "petabytes"), do: do_convert(bytes, "pb")
end
