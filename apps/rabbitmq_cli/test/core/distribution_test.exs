## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

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
