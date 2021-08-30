## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CommandModulesTest do
  use ExUnit.Case, async: false
  import TestHelper

  @subject RabbitMQ.CLI.Core.CommandModules

  setup_all do
    on_exit(fn ->
      set_scope(:none)
      Application.put_env(:rabbitmqctl, :commands, nil)
    end)
    :ok
  end

  test "command modules has existing commands" do
    assert @subject.load_commands(:all, %{})["duck"] ==
      RabbitMQ.CLI.Ctl.Commands.DuckCommand
  end

  test "command with multiple underscores shows up in map" do
    assert @subject.load_commands(:all, %{})["gray_goose"] ==
      RabbitMQ.CLI.Ctl.Commands.GrayGooseCommand
  end

  test "command modules does not have non-existent commands" do
    assert @subject.load_commands(:all, %{})["usurper"] == nil
  end

  test "non command modules do not show in command map" do
    assert @subject.load_commands(:all, %{})["ugly_duckling"] == nil
  end

  test "loaded commands are saved in env variable" do
    set_scope(:ctl)
    commands = @subject.module_map
    assert commands == @subject.module_map
    assert commands == Application.get_env(:rabbitmqctl, :commands)
  end

  test "load commands for current scope" do
    set_scope(:ctl)
    commands = @subject.load(%{})
    assert commands == @subject.load_commands(:ctl, %{})

    assert commands["duck"] == RabbitMQ.CLI.Ctl.Commands.DuckCommand
    assert commands["gray_goose"] == RabbitMQ.CLI.Ctl.Commands.GrayGooseCommand

    assert commands["stork"] == nil
    assert commands["heron"] == nil

    assert commands["crow"] == nil
    assert commands["raven"] == nil

    set_scope(:plugins)
    commands = @subject.load(%{})
    assert commands == @subject.load_commands(:plugins, %{})
    assert commands["duck"] == nil
    assert commands["gray_goose"] == nil

    assert commands["stork"] == RabbitMQ.CLI.Plugins.Commands.StorkCommand
    assert commands["heron"] == RabbitMQ.CLI.Plugins.Commands.HeronCommand

    assert commands["crow"] == nil
    assert commands["raven"] == nil
  end

  test "can set scopes inside command" do
    plugin_commands = @subject.load_commands(:plugins, %{})

    assert plugin_commands["duck"] == nil
    assert plugin_commands["gray_goose"] == nil

    assert plugin_commands["stork"] == RabbitMQ.CLI.Plugins.Commands.StorkCommand
    assert plugin_commands["heron"] == RabbitMQ.CLI.Plugins.Commands.HeronCommand

    assert plugin_commands["crow"] == nil
    assert plugin_commands["raven"] == nil

    # SeagullCommand has scopes() defined as [:plugins, :custom]
    assert plugin_commands["seagull"] == RabbitMQ.CLI.Seagull.Commands.SeagullCommand

    custom_commands = @subject.load_commands(:custom, %{})

    assert custom_commands["duck"] == nil
    assert custom_commands["gray_goose"] == nil

    assert custom_commands["stork"] == nil
    assert custom_commands["heron"] == nil

    assert custom_commands["crow"] == RabbitMQ.CLI.Custom.Commands.CrowCommand
    assert custom_commands["raven"] == RabbitMQ.CLI.Custom.Commands.RavenCommand

    # SeagullCommand has scopes() defined as [:plugins, :custom]
    assert custom_commands["seagull"] == RabbitMQ.CLI.Seagull.Commands.SeagullCommand

  end

  ## ------------------- commands/0 tests --------------------

  test "command_modules has existing commands" do
    set_scope(:ctl)
    @subject.load(%{})
    assert @subject.module_map["status"] == RabbitMQ.CLI.Ctl.Commands.StatusCommand
    assert @subject.module_map["environment"] == RabbitMQ.CLI.Ctl.Commands.EnvironmentCommand
  end

  test "command_modules does not have non-existent commands" do
    set_scope(:ctl)
    @subject.load(%{})
    assert @subject.module_map[:p_equals_np_proof] == nil
  end
end

# Mock command modules for Ctl

defmodule RabbitMQ.CLI.Ctl.Commands.DuckCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["duck"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end

defmodule RabbitMQ.CLI.Ctl.Commands.GrayGooseCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["gray_goose"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end

defmodule RabbitMQ.CLI.Ctl.Commands.UglyDucklingCommand do
end


# Mock command modules for Plugins

defmodule RabbitMQ.CLI.Plugins.Commands.StorkCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["stork"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end

defmodule RabbitMQ.CLI.Plugins.Commands.HeronCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["heron"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end

# Mock command modules for Custom

defmodule RabbitMQ.CLI.Custom.Commands.CrowCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["crow"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def scopes(), do: [:custom, ]
end

defmodule RabbitMQ.CLI.Custom.Commands.RavenCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["raven"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end

defmodule RabbitMQ.CLI.Seagull.Commands.SeagullCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["seagull"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def scopes(), do: [:plugins, :custom]
end


