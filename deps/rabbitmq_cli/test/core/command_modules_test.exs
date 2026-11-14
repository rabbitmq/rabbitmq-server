## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

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
    # true = Enum.take(RabbitMQ.CLI.Core.CommandModules.ctl_modules(), -15)
    # true = Enum.find_index(RabbitMQ.CLI.Core.CommandModules.ctl_modules(), fn x -> x == RabbitMQ.CLI.Ctl.Commands.DuckCommand end)

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

  for {name, specs, expected} <- [
        {"newer directory", [{:dir, ~c"1.0", ~c"old"}, {:dir, ~c"2.0", ~c"new"}], ~c"new"},
        {"newer directory first", [{:dir, ~c"2.0", ~c"new"}, {:dir, ~c"1.0", ~c"old"}], ~c"new"},
        {"equal directories", [{:dir, ~c"1.0", ~c"first"}, {:dir, ~c"1.0", ~c"second"}],
         ~c"first"},
        {"newer archive", [{:dir, ~c"1.0", ~c"old"}, {:ez, ~c"2.0", ~c"new"}], nil},
        {"older archive", [{:ez, ~c"1.0", ~c"old"}, {:dir, ~c"2.0", ~c"new"}], ~c"new"},
        {"equal archive", [{:dir, ~c"1.0", ~c"directory"}, {:ez, ~c"1.0", ~c"archive"}], nil}
      ] do
    @tag :tmp_dir
    @tag :plugin_selection
    test "plugin command loading selects #{name}", %{tmp_dir: root} do
      original_path = :code.get_path()
      dirs = Enum.map(["a", "b"], &Path.join(root, &1))
      enabled_file = Path.join(root, "enabled_plugins")
      File.write!(enabled_file, "[selected_cli_plugin].")

      try do
        for {dir, spec} <- Enum.zip(dirs, unquote(Macro.escape(specs))) do
          write_plugin(dir, spec)
        end

        opts = %{
          rabbitmq_home: :code.lib_dir(:rabbit),
          plugins_dir: Enum.join(dirs, RabbitMQ.CLI.Core.Platform.path_separator()),
          enabled_plugins_file: enabled_file,
          offline: true
        }

        case unquote(expected) do
          nil ->
            assert @subject.plugin_modules(opts) == []

          value ->
            assert @subject.plugin_modules(opts) == [:selected_cli_command]
            assert apply(:selected_cli_command, :value, []) == value
        end
      after
        Application.unload(:selected_cli_plugin)
        :code.purge(:selected_cli_command)
        :code.delete(:selected_cli_command)
        :code.set_path(original_path)
      end
    end
  end

  for type <- [:dir, :ez] do
    @tag :tmp_dir
    test "remote #{type} discovery loads commands from local plugin directories", %{tmp_dir: root} do
      :ok = Application.ensure_loaded(:rabbit)
      original_path = :code.get_path()
      keys = [:plugins_dir, :enabled_plugins_file]
      original_env = Enum.map(keys, &{&1, Application.fetch_env(:rabbit, &1)})
      local_dir = Path.join(root, "local")
      remote_dir = Path.join(root, "remote")
      enabled_file = Path.join(root, "enabled_plugins")
      File.write!(enabled_file, "[selected_cli_plugin].")
      write_plugin(local_dir, {:dir, ~c"1.0", ~c"local"})
      write_plugin(remote_dir, {unquote(type), ~c"1.0", ~c"remote"})

      try do
        Application.put_env(:rabbit, :plugins_dir, to_charlist(remote_dir))
        Application.put_env(:rabbit, :enabled_plugins_file, to_charlist(enabled_file))

        opts = %{
          node: node(),
          online: true,
          rabbitmq_home: :code.lib_dir(:rabbit),
          plugins_dir: local_dir
        }

        refute RabbitMQ.CLI.Plugins.Helpers.node_is_local?(opts)
        assert :code.which(:selected_cli_command) == :non_existing
        assert @subject.plugin_modules(opts) == [:selected_cli_command]
        assert apply(:selected_cli_command, :value, []) == ~c"local"
      after
        Application.unload(:selected_cli_plugin)
        :code.purge(:selected_cli_command)
        :code.delete(:selected_cli_command)
        :code.set_path(original_path)

        for {key, value} <- original_env do
          case value do
            {:ok, value} -> Application.put_env(:rabbit, key, value)
            :error -> Application.delete_env(:rabbit, key)
          end
        end
      end
    end
  end

  defp write_plugin(dir, {type, version, value}) do
    app_dir = "selected_cli_plugin-#{version}"

    props = [
      vsn: version,
      modules: [:selected_cli_command],
      applications: [:kernel, :stdlib, :rabbit]
    ]

    app = :io_lib.format(~c"~tp.", [{:application, :selected_cli_plugin, props}])

    forms = [
      {:attribute, 1, :module, :selected_cli_command},
      {:attribute, 1, :export, [value: 0]},
      {:function, 1, :value, 0, [{:clause, 1, [], [], [{:string, 1, value}]}]}
    ]

    {:ok, :selected_cli_command, beam} = :compile.forms(forms, [:binary])

    files = [
      {~c"#{app_dir}/ebin/selected_cli_plugin.app", IO.iodata_to_binary(app)},
      {~c"#{app_dir}/ebin/selected_cli_command.beam", beam}
    ]

    case type do
      :ez ->
        File.mkdir_p!(dir)
        {:ok, _} = :zip.create(~c"#{dir}/#{app_dir}.ez", files)

      :dir ->
        for {path, content} <- files do
          path = Path.join(dir, to_string(path))
          File.mkdir_p!(Path.dirname(path))
          File.write!(path, content)
        end
    end
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
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
end

defmodule RabbitMQ.CLI.Ctl.Commands.GrayGooseCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["gray_goose"]
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
end

defmodule RabbitMQ.CLI.Ctl.Commands.UglyDucklingCommand do
end

# Mock command modules for Plugins

defmodule RabbitMQ.CLI.Plugins.Commands.StorkCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["stork"]
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
end

defmodule RabbitMQ.CLI.Plugins.Commands.HeronCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["heron"]
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
end

# Mock command modules for Custom

defmodule RabbitMQ.CLI.Custom.Commands.CrowCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["crow"]
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
  def scopes(), do: [:custom]
end

defmodule RabbitMQ.CLI.Custom.Commands.RavenCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["raven"]
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
end

defmodule RabbitMQ.CLI.Seagull.Commands.SeagullCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["seagull"]
  def validate(_, _), do: :ok
  def merge_defaults(_, _), do: {[], %{}}
  def banner(_, _), do: ""
  def run(_, _), do: :ok
  def scopes(), do: [:plugins, :custom]
end
