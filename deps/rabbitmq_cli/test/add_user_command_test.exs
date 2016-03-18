## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule AddUserCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
			:net_kernel.stop()
		end)

    :ok
  end

  setup context do
    on_exit(context, fn -> delete_user(context[:user]) end)
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

	test "on an inappropriate number of arguments, print usage" do
		assert capture_io(fn -> AddUserCommand.add_user([], %{}) end) =~ ~r/Usage:/
		assert capture_io(fn -> AddUserCommand.add_user(["extra"], %{}) end) =~ ~r/Usage:/
		assert capture_io(fn -> AddUserCommand.add_user(["many", "extra", "commands"], %{}) end) =~ ~r/Usage:/
	end

	test "An invalid rabbitmq node throws a badrpc" do
		target = :jake@thedog
		:net_kernel.connect_node(target)
		opts = %{node: target}

		assert AddUserCommand.add_user(["user", "password"], opts) == {:badrpc, :nodedown}
	end

	@tag user: "someone", password: "password"
	test "default case completes successfully", context do
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == :ok
		assert list_users |> Enum.count(fn(record) -> record[:user] == context[:user] end) == 1
	end

	@tag user: "", password: "password"
	test "an empty username triggers usage message", context do
		assert capture_io(fn -> AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) end) =~ ~r/Usage:/
	end

	@tag user: "some_rando", password: ""
	test "an empty password succeeds", context do
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == :ok
	end

	@tag user: "someone", password: "password"
	test "adding an existing user returns an error", context do
		TestHelper.add_user(context[:user], context[:password])
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == {:error, {:user_already_exists, context[:user]}}
		assert list_users |> Enum.count(fn(record) -> record[:user] == context[:user] end) == 1
	end
end
