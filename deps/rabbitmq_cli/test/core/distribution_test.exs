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

alias RabbitMQ.CLI.Core.Distribution

defmodule DistributionTest do
  use ExUnit.Case, async: false

  setup_all do
    :net_kernel.stop()
    :ok
  end

  test "set cookie via environment variable" do
    on_exit(fn ->
      :net_kernel.stop()
      System.delete_env("RABBITMQ_ERLANG_COOKIE")
    end)
    try do
      :nocookie = Node.get_cookie()
    catch
      # one of net_kernel processes is not running ¯\_(ツ)_/¯
      :exit, _ -> :ok
    end
    System.put_env("RABBITMQ_ERLANG_COOKIE", "mycookie")
    opts = %{}
    Distribution.start(opts)
    :mycookie = Node.get_cookie()
  end

  test "set cookie via argument" do
    on_exit(fn ->
      :net_kernel.stop()
    end)
    try do
      :nocookie = Node.get_cookie()
    catch
      # one of net_kernel processes is not running ¯\_(ツ)_/¯
      :exit, _ -> :ok
    end
    opts = %{erlang_cookie: :mycookie}
    Distribution.start(opts)
    :mycookie = Node.get_cookie()
  end
end
