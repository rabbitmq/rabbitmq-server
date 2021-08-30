defmodule SchemaInfoCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.SchemaInfoCommand
  @default_timeout :infinity

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || @default_timeout
      }
    }
  end

  test "merge_defaults: adds all keys if none specificed", context do
    default_keys = ~w(name cookie active_replicas user_properties)

    {keys, _} = @command.merge_defaults([], context[:opts])
    assert default_keys == keys
  end

  test "merge_defaults: includes table headers by default", _context do
    {_, opts} = @command.merge_defaults([], %{})
    assert opts[:table_headers]
  end

  test "validate: returns bad_info_key on a single bad arg", context do
    assert @command.validate(["quack"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: returns multiple bad args return a list of bad info key values", context do
    assert @command.validate(["quack", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink, :quack]}}
  end

  test "validate: return bad_info_key on mix of good and bad args", context do
    assert @command.validate(["quack", "cookie"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert @command.validate(["access_mode", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert @command.validate(["access_mode", "oink", "name"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  @tag test_timeout: 0
  test "run: timeout causes command to return badrpc", context do
    assert run_command_to_list(@command, [["source_name"], context[:opts]]) ==
      {:badrpc, :timeout}
  end

  test "run: can filter info keys", context do
    wanted_keys = ~w(name access_mode)
    assert match?([[name: _, access_mode: _] | _], run_command_to_list(@command, [wanted_keys, context[:opts]]))
  end

  test "banner" do
    assert String.starts_with?(@command.banner([], %{node: "node@node"}), "Asking node")
  end
end
