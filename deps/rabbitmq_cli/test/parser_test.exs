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

## Mock command for command specific parser
defmodule RabbitMQ.CLI.Seagull.Commands.HerringGullCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["herring_gull"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def switches(), do: [herring: :string, garbage: :boolean]
  def aliases(), do: [h: :herring, g: :garbage]
end

defmodule RabbitMQ.CLI.Seagull.Commands.PacificGullCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["pacific_gull"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def switches(), do: []
  def aliases(), do: []
end

defmodule ParserTest do
  use ExUnit.Case, async: true
  import TestHelper

  @subject RabbitMQ.CLI.Core.Parser

  setup_all do
    Code.ensure_loaded(RabbitMQ.CLI.Seagull.Commands.HerringGullCommand)
    Code.ensure_loaded(RabbitMQ.CLI.Seagull.Commands.PacificGullCommand)
    set_scope(:seagull)
    on_exit(fn ->
      set_scope(:none)
    end)
    :ok
  end

  test "one arity 0 command, no options" do
    assert @subject.parse_global(["sandwich"]) == {["sandwich"], %{}, []}
  end

  test "one arity 1 command, no options" do
    assert @subject.parse_global(["sandwich", "pastrami"]) == {["sandwich", "pastrami"], %{}, []}
  end

  test "no commands, no options (empty string)" do
    assert @subject.parse_global([""]) == {[""], %{}, []}
  end

  test "no commands, no options (empty array)" do
    assert @subject.parse_global([]) == {[],%{}, []}
  end

  test "one arity 1 command, one double-dash quiet flag" do
    assert @subject.parse_global(["sandwich", "pastrami", "--quiet"]) ==
      {["sandwich", "pastrami"], %{quiet: true}, []}
  end

  test "one arity 1 command, one single-dash quiet flag" do
    assert @subject.parse_global(["sandwich", "pastrami", "-q"]) ==
      {["sandwich", "pastrami"], %{quiet: true}, []}
  end

  test "one arity 0 command, one single-dash node option" do
    assert @subject.parse_global(["sandwich", "-n", "rabbitmq@localhost"]) ==
      {["sandwich"], %{node: :"rabbitmq@localhost"}, []}
  end

  test "one arity 1 command, one single-dash node option" do
    assert @subject.parse_global(["sandwich", "pastrami", "-n", "rabbitmq@localhost"]) ==
      {["sandwich", "pastrami"], %{node: :"rabbitmq@localhost"}, []}
  end

  test "one arity 1 command, one single-dash node option and one quiet flag" do
    assert @subject.parse_global(["sandwich", "pastrami", "-n", "rabbitmq@localhost", "--quiet"]) ==
      {["sandwich", "pastrami"], %{node: :"rabbitmq@localhost", quiet: true}, []}
  end

  test "single-dash node option before command" do
    assert @subject.parse_global(["-n", "rabbitmq@localhost", "sandwich", "pastrami"]) ==
      {["sandwich", "pastrami"], %{node: :"rabbitmq@localhost"}, []}
  end

  test "no commands, one double-dash node option" do
    assert @subject.parse_global(["--node=rabbitmq@localhost"]) == {[], %{node: :"rabbitmq@localhost"}, []}
  end

  test "no commands, one integer --timeout value" do
    assert @subject.parse_global(["--timeout=600"]) == {[], %{timeout: 600}, []}
  end

  test "no commands, one string --timeout value is invalid" do
    assert @subject.parse_global(["--timeout=sandwich"]) == {[], %{}, [{"--timeout", "sandwich"}]}
  end

  test "no commands, one float --timeout value is invalid" do
    assert @subject.parse_global(["--timeout=60.5"]) == {[], %{}, [{"--timeout", "60.5"}]}
  end

  test "no commands, one integer -t value" do
    assert @subject.parse_global(["-t", "600"]) == {[], %{timeout: 600}, []}
  end

  test "no commands, one string -t value is invalid" do
    assert @subject.parse_global(["-t", "sandwich"]) == {[], %{}, [{"-t", "sandwich"}]}
  end

  test "no commands, one float -t value is invalid" do
    assert @subject.parse_global(["-t", "60.5"]) == {[], %{}, [{"-t", "60.5"}]}
  end

  test "no commands, one single-dash -p option" do
    assert @subject.parse_global(["-p", "sandwich"]) == {[], %{vhost: "sandwich"}, []}
  end

  test "global parse returns command-specific arguments as invalid" do
    command_line = ["seagull", "--herring=atlantic", "-g", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.HerringGullCommand
    {args, options, invalid} = @subject.parse_global(command_line)
    assert {args, options, invalid} ==
      {["seagull"], %{vhost: "my_vhost"}, [{"--herring", nil}, {"-g", nil}]}
  end

  test "command-specific parse can parse command switches" do
    command_line = ["seagull", "--herring=atlantic", "-g", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.HerringGullCommand
    assert @subject.parse_command_specific(command_line, command) ==
      {["seagull"], %{vhost: "my_vhost", herring: "atlantic", garbage: true}, []}
  end

  test "command-specific switches and aliases are optional" do
    command_line = ["seagull", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.PacificGullCommand
    assert @subject.parse_command_specific(command_line, command) ==
      {["seagull"], %{vhost: "my_vhost"}, []}
  end

  test "parse/1 locates command" do
    command_line = ["pacific_gull", "fly", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.PacificGullCommand
    assert @subject.parse(command_line) ==
      {command, "pacific_gull", ["fly"], %{vhost: "my_vhost"}, []}
  end

  test "parse/1 returns :no_command for empty arguments" do
    command_line = ["-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.PacificGullCommand
    assert @subject.parse(command_line) ==
      {:no_command, "", [], %{vhost: "my_vhost"}, []}
  end

  test "parse/1 returns :no_command and command name if command not found" do
    command_line = ["atlantic_gull", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.PacificGullCommand
    assert @subject.parse(command_line) ==
      {:no_command, "atlantic_gull", [], %{vhost: "my_vhost"}, []}
  end

  test "parse/1 returns :no command if command specific options come before the command" do
    command_line = ["--herring", "atlantic", "herring_gull", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.HerringGullCommand
    assert @subject.parse(command_line) ==
      {:no_command, "atlantic", ["herring_gull"],
       %{vhost: "my_vhost"}, [{"--herring", nil}]}
  end

  test "parse/1 returns command with command specific options" do
    command_line = ["herring_gull", "--herring", "atlantic",
                    "-g", "fly", "-p", "my_vhost"]
    command = RabbitMQ.CLI.Seagull.Commands.HerringGullCommand
    assert @subject.parse(command_line) ==
      {command, "herring_gull", ["fly"],
       %{vhost: "my_vhost", herring: "atlantic", garbage: true}, []}
  end

  test "parse/1 returns invalid options for command as invalid" do
    command_line = ["pacific_gull", "fly",
                    "--herring", "atlantic",
                    "-p", "my_vhost"]
    pacific_gull = RabbitMQ.CLI.Seagull.Commands.PacificGullCommand
    assert @subject.parse(command_line) ==
      {pacific_gull, "pacific_gull", ["fly", "atlantic"],
       %{vhost: "my_vhost"},
       [{"--herring", nil}]}
  end

end
