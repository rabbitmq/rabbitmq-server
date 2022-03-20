## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.ANSI do
  def bright(string) do
    "#{IO.ANSI.bright()}#{string}#{IO.ANSI.reset()}"
  end

  def red(string) do
    "#{IO.ANSI.red()}#{string}#{IO.ANSI.reset()}"
  end

  def yellow(string) do
    "#{IO.ANSI.yellow()}#{string}#{IO.ANSI.reset()}"
  end

  def magenta(string) do
    "#{IO.ANSI.magenta()}#{string}#{IO.ANSI.reset()}"
  end

  def bright_red(string) do
    "#{IO.ANSI.bright()}#{IO.ANSI.red()}#{string}#{IO.ANSI.reset()}"
  end

  def bright_yellow(string) do
    "#{IO.ANSI.bright()}#{IO.ANSI.yellow()}#{string}#{IO.ANSI.reset()}"
  end

  def bright_magenta(string) do
    "#{IO.ANSI.bright()}#{IO.ANSI.magenta()}#{string}#{IO.ANSI.reset()}"
  end
end
