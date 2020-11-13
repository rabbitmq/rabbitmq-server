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


defmodule ClearVhostLimitsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClearVhostLimitsCommand
  @vhost "test1"
  @definition "{\"max-connections\":100}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    add_vhost @vhost

    on_exit([], fn ->
      delete_vhost @vhost

    end)

    :ok
  end

  setup context do
    on_exit(context, fn ->
      clear_vhost_limits(context[:vhost])
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname()
      }
    }
  end

  test "merge_defaults: adds default vhost if missing" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
  end

  test "merge_defaults: does not change defined vhost" do
    assert @command.merge_defaults([], %{vhost: "test_vhost"}) == {[], %{vhost: "test_vhost"}}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["this", "is", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: providing zero arguments and no options passes validation" do
    assert @command.validate([], %{}) == :ok
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end


  @tag vhost: @vhost
  test "run: if limits exist, returns ok and clears them", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    :ok = set_vhost_limits(context[:vhost], @definition)

    assert get_vhost_limits(context[:vhost]) != []

    assert @command.run(
      [],
      vhost_opts
    ) == :ok

    assert get_vhost_limits(context[:vhost]) == %{}
  end

  @tag vhost: "bad-vhost"
  test "run: a non-existent vhost returns an error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}
  end

  @tag vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    s = @command.banner(
      [],
      vhost_opts
    )

    assert s =~ ~r/Clearing vhost \"#{context[:vhost]}\" limits .../
  end

end
