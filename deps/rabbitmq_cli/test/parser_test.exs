defmodule ParserTest do
  use ExUnit.Case, async: true

  test "one arity 0 command, no options" do
    assert Parser.parse(["sandwich"]) == {["sandwich"], []}
  end

  test "one arity 1 command, no options" do
    assert Parser.parse(["sandwich", "pastrami"]) == {["sandwich", "pastrami"], []}
  end

  test "one arity 1 command, one double-dash quiet flag" do
    assert Parser.parse(["sandwich", "pastrami", "--quiet"]) == 
      {["sandwich", "pastrami"], [quiet: true]}
  end

  test "one arity 1 command, one single-dash quiet flag" do
    assert Parser.parse(["sandwich", "pastrami", "-q"]) == 
      {["sandwich", "pastrami"], [quiet: true]}
  end

  test "one arity 0 command, one single-dash node option" do
    assert Parser.parse(["sandwich", "-n", "rabbitmq@localhost"]) == 
      {["sandwich"], [node: "rabbitmq@localhost"]}
  end

  test "one arity 1 command, one single-dash node option" do
    assert Parser.parse(["sandwich", "pastrami", "-n", "rabbitmq@localhost"]) == 
      {["sandwich", "pastrami"], [node: "rabbitmq@localhost"]}
  end

  test "one arity 1 command, one single-dash node option and one quiet flag" do
    assert Parser.parse(["sandwich", "pastrami", "-n", "rabbitmq@localhost", "--quiet"]) == 
      {["sandwich", "pastrami"], [node: "rabbitmq@localhost", quiet: true]}
  end

  test "single-dash node option before command" do
    assert Parser.parse(["-n", "rabbitmq@localhost", "sandwich", "pastrami"]) == 
      {["sandwich", "pastrami"], [node: "rabbitmq@localhost"]}
  end

  test "no commands, one double-dash node option" do
    assert Parser.parse(["-n=rabbitmq@localhost"]) == {[], [node: "rabbitmq@localhost"]}
  end

  test "no commands, no options (empty string)" do
    assert Parser.parse([""]) == {[], []}
  end

  test "no commands, no options (empty array)" do
    assert Parser.parse([]) == {[], []}
  end
end
