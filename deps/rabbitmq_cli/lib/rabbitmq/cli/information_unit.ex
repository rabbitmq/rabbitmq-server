## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.InformationUnit do
  require MapSet

  @kilobyte_bytes 1000
  @megabyte_bytes @kilobyte_bytes * 1000
  @gigabyte_bytes @megabyte_bytes * 1000
  @terabyte_bytes @gigabyte_bytes * 1000

  def known_units() do
    MapSet.new([
      "bytes",
      "kb",
      "kilobytes",
      "mb",
      "megabytes",
      "gb",
      "gigabytes",
      "tb",
      "terabytes"
    ])
  end

  def parse(val) do
    :rabbit_resource_monitor_misc.parse_information_unit(val)
  end

  def convert(bytes, "bytes") do
    bytes
  end

  def convert(bytes, unit) do
    do_convert(bytes, String.downcase(unit))
  end

  def known_unit?(val) do
    MapSet.member?(known_units(), String.downcase(val))
  end

  defp do_convert(bytes, "kb") do
    Float.round(bytes / @kilobyte_bytes, 4)
  end

  defp do_convert(bytes, "kilobytes"), do: do_convert(bytes, "kb")

  defp do_convert(bytes, "mb") do
    Float.round(bytes / @megabyte_bytes, 4)
  end

  defp do_convert(bytes, "megabytes"), do: do_convert(bytes, "mb")

  defp do_convert(bytes, "gb") do
    Float.round(bytes / @gigabyte_bytes, 4)
  end

  defp do_convert(bytes, "gigabytes"), do: do_convert(bytes, "gb")

  defp do_convert(bytes, "tb") do
    Float.round(bytes / @terabyte_bytes, 4)
  end

  defp do_convert(bytes, "terabytes"), do: do_convert(bytes, "tb")
end
