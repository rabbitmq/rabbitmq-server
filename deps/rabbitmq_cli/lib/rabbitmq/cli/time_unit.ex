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

defmodule RabbitMQ.CLI.TimeUnit do
  require MapSet

  @days_seconds 86400
  @weeks_seconds @days_seconds * 7
  @months_seconds @days_seconds * (365 / 12)
  @years_seconds @days_seconds * 365

  def known_units() do
    MapSet.new([
      "days",
      "weeks",
      "months",
      "years"
    ])
  end

  def convert(time, unit) do
    do_convert(time, String.downcase(unit))
  end

  def known_unit?(val) do
    MapSet.member?(known_units(), String.downcase(val))
  end

  defp do_convert(time, "days") do
    time * @days_seconds
  end

  defp do_convert(time, "weeks") do
    time * @weeks_seconds
  end

  defp do_convert(time, "months") do
    time * @months_seconds
  end

  defp do_convert(time, "years") do
    time * @years_seconds
  end

end
