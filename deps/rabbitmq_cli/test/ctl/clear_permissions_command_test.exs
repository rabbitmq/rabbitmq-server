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


defmodule ClearPermissionsTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands. ClearPermissionsCommand
  @user     "user1"
  @password "password"
  @default_vhost "/"
  @specific_vhost "vhost1"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_user(@user, @password)
    add_vhost(@specific_vhost)

    on_exit([], fn ->
      delete_user(@user)
      delete_vhost(@specific_vhost)
    end)

    :ok
  end

  setup context do
    set_permissions(@user, @default_vhost, ["^#{@user}-.*", ".*", ".*"])
    set_permissions(@user, @specific_vhost, ["^#{@user}-.*", ".*", ".*"])

    {
      :ok,
      opts: %{node: get_rabbit_hostname(), vhost: context[:vhost]}
    }
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  test "validate: argument count validates" do
    assert @command.validate(["one"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag user: "fake_user"
  test "run: can't clear permissions for non-existing user", context do
    assert @command.run([context[:user]], context[:opts]) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user, vhost: @default_vhost
  test "run: a valid username clears permissions", context do
    assert @command.run([context[:user]], context[:opts]) == :ok

    assert list_permissions(@default_vhost)
    |> Enum.filter(fn(record) -> record[:user] == context[:user] end) == []
  end

  test "run: on an invalid node, return a badrpc message" do
    arg = ["some_name"]
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run(arg, opts))
  end

  @tag user: @user, vhost: @specific_vhost
  test "run: on a valid specified vhost, clear permissions", context do
    assert @command.run([context[:user]], context[:opts]) == :ok

    assert list_permissions(context[:vhost])
    |> Enum.filter(fn(record) -> record[:user] == context[:user] end) == []
  end

  @tag user: @user, vhost: "bad_vhost"
  test "run: on an invalid vhost, return no_such_vhost error", context do
    assert @command.run([context[:user]], context[:opts]) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag user: @user, vhost: @specific_vhost
  test "banner", context do
    s = @command.banner([context[:user]], context[:opts])

    assert s =~ ~r/Clearing permissions/
    assert s =~ ~r/\"#{context[:user]}\"/
    assert s =~ ~r/\"#{context[:vhost]}\"/
  end
end
