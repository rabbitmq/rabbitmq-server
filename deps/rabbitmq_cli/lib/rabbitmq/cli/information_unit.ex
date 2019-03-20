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
