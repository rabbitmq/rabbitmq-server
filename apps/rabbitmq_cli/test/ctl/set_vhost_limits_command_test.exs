## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetVhostLimitsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetVhostLimitsCommand

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

    vhost = context[:vhost] || @vhost

    clear_vhost_limits(vhost)

    on_exit(context, fn ->
      clear_vhost_limits(vhost)
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        vhost: vhost
      },
      definition: context[:definition] || @definition,
      vhost: vhost
    }
  end

  test "merge_defaults: a well-formed command with no vhost runs against the default" do
    assert match?({_, %{vhost: "/"}}, @command.merge_defaults([], %{}))
  end

  test "merge_defaults: does not change defined vhost" do
    assert match?({[], %{vhost: "test_vhost"}}, @command.merge_defaults([], %{vhost: "test_vhost"}))
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: a well-formed, host-specific command returns okay", context do
    assert @command.run(
      [context[:definition]],
      context[:opts]
    ) == :ok

    assert_limits(context)
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run([@definition], opts))
  end

  @tag vhost: "bad-vhost"
  test "run: providing a non-existent vhost reports an error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:definition]],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  test "run: an invalid definition returns a JSON decoding error", context do
    assert match?({:error_string, _},
      @command.run(["bad_value"], context[:opts]))

    assert get_vhost_limits(context[:vhost]) == %{}
  end

  test "run: invalid limit returns an error", context do
    assert @command.run(
      ["{\"foo\":\"bar\"}"],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\nUnrecognised terms [{<<"foo">>,<<"bar">>}] in limits\n'}

    assert get_vhost_limits(context[:vhost]) == %{}
  end

  test "run: an empty JSON object definition unsets all limits for vhost", context do

    assert @command.run(
      [@definition],
      context[:opts]
    ) == :ok

    assert_limits(context)

    assert @command.run(
      ["{}"],
      context[:opts]
    ) == :ok

    assert get_vhost_limits(context[:vhost]) == %{}
  end

  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:definition]], vhost_opts)
      == "Setting vhost limits to \"#{context[:definition]}\" for vhost \"#{context[:vhost]}\" ..."
  end

  defp assert_limits(context) do
    limits = get_vhost_limits(context[:vhost])
    assert {:ok, limits} == JSON.decode(context[:definition])
  end
end
