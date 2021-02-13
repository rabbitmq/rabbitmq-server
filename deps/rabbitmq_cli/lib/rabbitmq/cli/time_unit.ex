## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
