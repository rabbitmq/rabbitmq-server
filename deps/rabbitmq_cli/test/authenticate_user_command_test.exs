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


defmodule AuthenticateUserCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @user     "user1"
  @password "password"

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
    add_user(@user, @password)
    on_exit(context, fn -> delete_user(@user) end)
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "validate: invalid number of arguments returns a validation failure" do
    assert AuthenticateUserCommand.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert AuthenticateUserCommand.validate(["user"], %{}) == {:validation_failure, :not_enough_args}
    assert AuthenticateUserCommand.validate(["user", "password", "extra"], %{}) ==
      {:validation_failure, :too_many_args}
  end
  test "validate: correct arguments return :ok" do
    assert AuthenticateUserCommand.validate(["user", "password"], %{}) == :ok
  end

  @tag user: @user, password: @password
  test "run: a valid username and password returns okay", context do
    assert {:ok, _} = AuthenticateUserCommand.run([context[:user], context[:password]], context[:opts])
  end

  test "run: An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}
    assert AuthenticateUserCommand.run(["user", "password"], opts) == {:badrpc, :nodedown}
  end

  @tag user: @user, password: "treachery"
  test "run: a valid username and invalid password returns refused", context do
    assert {:refused, _, _, _} = AuthenticateUserCommand.run([context[:user], context[:password]], context[:opts])
  end

  @tag user: "interloper", password: @password
  test "run: an invalid username returns refused", context do
    assert {:refused, _, _, _} = AuthenticateUserCommand.run([context[:user], context[:password]], context[:opts])
  end

  @tag user: @user, password: @password
  test "banner", context do
    AuthenticateUserCommand.banner([context[:user], context[:password]], context[:opts])
      =~ ~r/Authenticating user "#{context[:user]}" \.\.\./
  end
end
