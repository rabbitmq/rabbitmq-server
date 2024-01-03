## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Core.ANSI do
  def bright(string) do
    maybe_colorize([:bright, string])
  end

  def red(string) do
    maybe_colorize([:red, string])
  end

  def yellow(string) do
    maybe_colorize([:yellow, string])
  end

  def magenta(string) do
    maybe_colorize([:magenta, string])
  end

  def bright_red(string) do
    maybe_colorize([:bright, :red, string])
  end

  def bright_yellow(string) do
    maybe_colorize([:bright, :yellow, string])
  end

  def bright_magenta(string) do
    maybe_colorize([:bright, :magenta, string])
  end

  defp maybe_colorize(ascii_esc_and_string) do
    ascii_esc_and_string
    |> IO.ANSI.format()
    |> IO.chardata_to_string()
  end
end
