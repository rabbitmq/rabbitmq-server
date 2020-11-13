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

defmodule RabbitMQ.CLI.Core.Platform do
  def path_separator() do
    case :os.type() do
      {:unix, _} -> ":"
      {:win32, _} -> ";"
    end
  end

  def line_separator() do
    case :os.type() do
      {:unix, _}  -> "\n"
      {:win32, _} -> "\r\n"
    end
  end

  def os_name({:unix, :linux}) do
    "Linux"
  end
  def os_name({:unix, :darwin}) do
    "MacOS"
  end
  def os_name({:unix, :freebsd}) do
    "FreeBSD"
  end
  def os_name({:unix, name}) do
    name |> to_string |> String.capitalize
  end
  def os_name({:win32, _}) do
    "Windows"
  end
end
