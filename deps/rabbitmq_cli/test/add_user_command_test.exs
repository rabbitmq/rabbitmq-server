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
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :net_kernel.connect_node(context[:target])
    on_exit(context, fn ->
      delete_user(context[:user])
      :erlang.disconnect_node(context[:target])
    end)
    {:ok, opts: %{node: context[:target]}}
  end

	test "on an inappropriate number of arguments, print usage" do
		assert capture_io(fn -> AddUserCommand.add_user([], %{}) end) =~ ~r/Usage:/
		assert capture_io(fn -> AddUserCommand.add_user(["extra"], %{}) end) =~ ~r/Usage:/
		assert capture_io(fn -> AddUserCommand.add_user(["many", "extra", "commands"], %{}) end) =~ ~r/Usage:/
	end

	@tag target: get_rabbit_hostname, user: "someone", password: "password"
	test "default case completes successfully", context do
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == :ok
	end

	@tag target: :jake@thedog, user: "someone", password: "password"
	test "An invalid rabbitmq node throws a badrpc", context do
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == {:badrpc, :nodedown}
	end

	@tag target: get_rabbit_hostname, user: "", password: "password"
	test "an empty username triggers usage message", context do
		assert capture_io(fn -> AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) end) =~ ~r/Usage:/
	end

	@tag target: get_rabbit_hostname, user: "some_rando", password: ""
	test "an empty password succeeds", context do
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == :ok
	end

	@tag target: get_rabbit_hostname, user: "someone", password: "password"
	test "adding an existing user returns an error", context do
		TestHelper.add_user(context[:user], context[:password])
		assert AddUserCommand.add_user([context[:user], context[:password]], context[:opts]) == {:error, {:user_already_exists, context[:user]}}
	end
end
