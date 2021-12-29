## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
    "macOS"
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
