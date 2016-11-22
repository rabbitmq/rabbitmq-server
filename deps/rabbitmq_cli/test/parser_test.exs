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


defmodule ParserTest do
  use ExUnit.Case, async: true

  @subject RabbitMQ.CLI.Core.Parser

  test "one arity 0 command, no options" do
    assert @subject.parse(["sandwich"]) == {["sandwich"], %{}, []}
  end

  test "one arity 1 command, no options" do
    assert @subject.parse(["sandwich", "pastrami"]) == {["sandwich", "pastrami"], %{}, []}
  end

  test "no commands, no options (empty string)" do
    assert @subject.parse([""]) == {[], %{}, []}
  end

  test "no commands, no options (empty array)" do
    assert @subject.parse([]) == {[],%{}, []}
  end

  test "one arity 1 command, one double-dash quiet flag" do
    assert @subject.parse(["sandwich", "pastrami", "--quiet"]) ==
      {["sandwich", "pastrami"], %{quiet: true}, []}
  end

  test "one arity 1 command, one single-dash quiet flag" do
    assert @subject.parse(["sandwich", "pastrami", "-q"]) ==
      {["sandwich", "pastrami"], %{quiet: true}, []}
  end

  test "one arity 0 command, one single-dash node option" do
    assert @subject.parse(["sandwich", "-n", "rabbitmq@localhost"]) ==
      {["sandwich"], %{node: :"rabbitmq@localhost"}, []}
  end

  test "one arity 1 command, one single-dash node option" do
    assert @subject.parse(["sandwich", "pastrami", "-n", "rabbitmq@localhost"]) ==
      {["sandwich", "pastrami"], %{node: :"rabbitmq@localhost"}, []}
  end

  test "one arity 1 command, one single-dash node option and one quiet flag" do
    assert @subject.parse(["sandwich", "pastrami", "-n", "rabbitmq@localhost", "--quiet"]) ==
      {["sandwich", "pastrami"], %{node: :"rabbitmq@localhost", quiet: true}, []}
  end

  test "single-dash node option before command" do
    assert @subject.parse(["-n", "rabbitmq@localhost", "sandwich", "pastrami"]) ==
      {["sandwich", "pastrami"], %{node: :"rabbitmq@localhost"}, []}
  end

  test "no commands, one double-dash node option" do
    assert @subject.parse(["--node=rabbitmq@localhost"]) == {[], %{node: :"rabbitmq@localhost"}, []}
  end

  test "no commands, one integer --timeout value" do
    assert @subject.parse(["--timeout=600"]) == {[], %{timeout: 600}, []}
  end

  test "no commands, one string --timeout value is invalid" do
    assert @subject.parse(["--timeout=sandwich"]) == {[], %{}, [{"--timeout", "sandwich"}]}
  end

  test "no commands, one float --timeout value is invalid" do
    assert @subject.parse(["--timeout=60.5"]) == {[], %{}, [{"--timeout", "60.5"}]}
  end

  test "no commands, one integer -t value" do
    assert @subject.parse(["-t", "600"]) == {[], %{timeout: 600}, []}
  end

  test "no commands, one string -t value is invalid" do
    assert @subject.parse(["-t", "sandwich"]) == {[], %{}, [{"-t", "sandwich"}]}
  end

  test "no commands, one float -t value is invalid" do
    assert @subject.parse(["-t", "60.5"]) == {[], %{}, [{"-t", "60.5"}]}
  end

  test "no commands, one single-dash -p option" do
    assert @subject.parse_global(["-p", "sandwich"]) == {[], %{vhost: "sandwich"}, []}
  end
end
