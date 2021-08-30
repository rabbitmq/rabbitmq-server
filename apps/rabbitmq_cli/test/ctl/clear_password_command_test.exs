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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ClearPasswordCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands. ClearPasswordCommand
  @user     "user1"
  @password "password"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    add_user(@user, @password)
    on_exit(context, fn -> delete_user(@user) end)
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: argument count is correct" do
    assert @command.validate(["username"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["username", "extra"], %{}) ==
        {:validation_failure, :too_many_args}
  end

  @tag user: @user, password: @password
  test "run: a valid username clears the password and returns okay", context do
    assert @command.run([context[:user]], context[:opts]) == :ok
    assert {:refused, _, _, _} = authenticate_user(context[:user], context[:password])
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["user"], opts))
  end

  @tag user: "interloper"
  test "run: An invalid username returns a no-such-user error message", context do
    assert @command.run([context[:user]], context[:opts]) == {:error, {:no_such_user, "interloper"}}
  end

  @tag user: @user
  test "banner", context do
    s = @command.banner([context[:user]], context[:opts])

    assert s =~ ~r/Clearing password/
    assert s =~ ~r/"#{context[:user]}"/
  end
end
