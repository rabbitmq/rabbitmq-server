## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.


defmodule EnableClassicMirroringCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.EnableClassicMirroringCommand

  @vhost "test1"
  @policy_name "ha-all-test-policy"
  @pattern "\.*"
  @key "ha-mode"
  @value "{\"ha-mode\":\"all\"}"
  @component_name "policy"



  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost @vhost
    enable_feature_flag :runtime_parameters_acl
    :ok = :rpc.call(get_rabbit_hostname(), :rabbit_runtime_parameters_acl, :ensure_table, [])
    disallow_parameter @vhost, @component_name, @key

    on_exit([], fn ->
      allow_parameter @vhost, @component_name, @key
      delete_vhost @vhost
    end)

    :ok
  end

  setup _context do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: no positional arguments passes" do
    assert @command.validate([], %{}) == :ok
  end

  test "validate: too many positional arguments fails" do
    assert @command.validate(["soft", "extra"], %{}) ==
      {:validation_failure, :too_many_args}
  end

  test "run: disallowed ha policy is successfully allowed to be set", context do
    expected_error_string = 'classic mirroring is not allowed in vhost \'#{@vhost}\''
    assert match?({:error_string, ^expected_error_string},
                  set_policy(@vhost, @policy_name, @pattern, @value))
    assert match?(:ok, @command.run([], Map.merge(context[:opts], %{vhost: @vhost})))
    assert match?(:ok, set_policy(@vhost, @policy_name, @pattern, @value))
  end

end
