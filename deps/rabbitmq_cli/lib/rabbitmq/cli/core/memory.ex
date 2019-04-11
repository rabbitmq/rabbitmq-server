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

defmodule RabbitMQ.CLI.Core.Memory do
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
end
