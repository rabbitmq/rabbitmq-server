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


defmodule TablePrintTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO

  setup_all do
    :ok
  end

####### print_dashed_line

  test "on a 2-tuple, prints the width plus three characters" do
    input = {4,3}
    assert capture_io(fn -> TablePrint.print_dashed_line(input) end) == "----------\n"
  end

  test "on a 3-tuple, prints the width plus six characters" do
    input = {2,5,2}
    assert capture_io(fn -> TablePrint.print_dashed_line(input) end) == "---------------\n"
  end

  test "on nil, does nothing and returns nil" do
    input = nil
    assert TablePrint.print_dashed_line(input) == nil
    assert capture_io(fn -> TablePrint.print_dashed_line(input) end) == ""
  end

  test "on an empty tuple, does nothing and returns nil" do
    input = {}
    assert TablePrint.print_dashed_line(input) == nil
    assert capture_io(fn -> TablePrint.print_dashed_line(input) end) == ""
  end

####### column_widths

  test "on a list of 2-tuples, returns the maximum for each field" do
    input = [{'Bruce', 'Wayne'}, {'Elijah', 'Snow'}, {'Jenny', 'Sparks'}]
    assert TablePrint.column_widths(input, 2) == {6,6}
  end

  test "on a list of 3-tuples, return the maximum for each field" do
    input = [{'this', 'is', 'Sparta'}, {'but', 'this', 'is not'}, {'this', 'is', 'Dubuque'}]
    assert TablePrint.column_widths(input, 3) == {4,4,7}
  end

  test "does not choke on empty fields" do
    input = [{'sandwich', ''}, {'condiments', ''}]
    assert TablePrint.column_widths(input, 2) == {10,0}
  end

  test "on an empty list, returns a tuple of zeros" do
    input = []
    assert TablePrint.column_widths(input, 4) == {0,0,0,0}
  end

  test "on an 0-column input, returns an empty tuple" do
    input = [{}]
    assert TablePrint.column_widths(input, 0) == {}
  end

  test "when inputs have more columns than ncols, returns tuple of ncols size" do
    input = [{'sandwich', 'pastrami'}]
    assert TablePrint.column_widths(input, 1) == {8}
  end

  test "when inputs have fewer columns than ncols, returns tuple of tuple size" do
    input = [{'sandwich', 'pastrami'}]
    assert TablePrint.column_widths(input, 3) == {8,8}
  end

####### generate_io_format

  test "On a set of three inputs, generate a string with two dividers" do
    field = {:sandwich, :pastrami, :rye}
    width = {8, 8, 3}
    assert TablePrint.generate_io_format(field, width) == "~-8w | ~-8w | ~-3w\n"
  end

  test "On a set of mixed inputs, generates the appropriate format types" do
    field1 = {:sandwich, 'pastrami', "rye"}
    width1 = {8, 8, 3}
    assert TablePrint.generate_io_format(field1, width1) == "~-8w | ~-8s | ~-3s\n"

    field2 = {:radians, 2, 3.141592}
    width2 = {7, 2, 8}
    assert TablePrint.generate_io_format(field2, width2) == "~-7w | ~-2w | ~-8w\n"
  end

  test "If nfields > nwidths, format string should have nfields elements" do
    field = {:sandwich, :pastrami, :rye}
    width = {8, 8}
    assert TablePrint.generate_io_format(field, width) == "~-8w | ~-8w\n"
  end

  test "If nfields < nwidths, format string should have nwidths elements" do
    field = {:sandwich, :pastrami}
    width = {8, 8, 3}
    assert TablePrint.generate_io_format(field, width) == "~-8w | ~-8w\n"
  end

  test "An empty field tuple or width set creates an empty format string" do
    field1 = {}
    width1 = {2}
    assert TablePrint.generate_io_format(field1, width1) == ""

    field1 = {:sandwich}
    width1 = {}
    assert TablePrint.generate_io_format(field1, width1) == ""
  end

####### tuple_to_table

  test "A list of tuples of the same length should print as a table" do
    input = [{:rabbitmq_routing_node_stamp,
          'Stamps a message with the name of the RabbitMQ node that accepted it from the client.',
          []},
      {:rabbit,'RabbitMQ','0.0.0'},
      {:ranch,'Socket acceptor pool for TCP protocols.','1.2.1'},
      {:mnesia,'MNESIA  CXC 138 12','4.13.2'},
      {:rabbit_common,[],[]},
      {:kernel,'ERTS  CXC 138 10','4.1.1'}]

    name = "Lots of RabbitMQage"

    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end) =~ ~r/Lots of RabbitMQage:\n/
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end) =~ ~r/-{124}\n/

    # The input above should print 8 lines. The extra line comes from the split.
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end)
    |> String.split("\n")
    |> length == 9
  end

  test "A nil input does not print a table" do
    input = nil
    name = "nope"
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end) == ""
  end

  test "An empty list does not print a table" do
    input = []
    name = "nope"
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end) == ""
  end

  test "An empty table name prints a table with no name" do
    input = [{:rabbit,'RabbitMQ','0.0.0'}]
    name = ""
    refute capture_io(fn -> TablePrint.tuple_to_table(input, name) end) =~ ~r/:\n/
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end) =~ ~r/-{25}\n/
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end)
    |> String.split("\n")
    |> length == 3
  end

  test "A nil table name prints a table with an empty string for a name" do
    input = [{:rabbit,'RabbitMQ','0.0.0'}]
    name = nil
    refute capture_io(fn -> TablePrint.tuple_to_table(input, name) end) =~ ~r/nil/
    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end) =~ ~r/-{25}\n/

    assert capture_io(fn -> TablePrint.tuple_to_table(input, name) end)
    |> String.split("\n")
    |> length == 3
  end

  test "A list of uneven tuples raises an exception" do
    input = [{:rabbit,'RabbitMQ','0.0.0'}, {:breakthis, 'nooooooooooo'}]
    name = nil
    assert_raise(ArgumentError, fn -> TablePrint.tuple_to_table(input, name) end)
  end

####### print_table

  test "A valid result prints out a table" do
    input = [{:procs, [{:rabbit,'RabbitMQ','0.0.0'}, {:wabbit, 'WabbitMQ', '0.0.1'}]}]
    name = "Processes"

    assert capture_io(fn -> TablePrint.print_table(input, :procs, name) end)
    |> String.split("\n")
    |> length == 5
  end

  test "An empty result does nothing" do
    input = []
    name = "nope"
    assert capture_io(fn -> TablePrint.print_table(input, :procs, name) end) == ""
  end

  test "A nil result does nothing" do
    input = nil
    name = "nope"
    assert capture_io(fn -> TablePrint.print_table(input, :procs, name) end) == ""
  end

  test "An empty result field prints the field name, followed by 'None'" do
    input = [{:procs, []}]
    name = "Processes"
    assert capture_io(fn -> TablePrint.print_table(input, :procs, name) end) == "Processes: None\n"
  end

  test "A result that doesn't match the table does not get printed" do
    input = [
              {:unprintable, [{'haha', 'nope', 'nopenope'}]},
              {:procs, [{:rabbit,'RabbitMQ','0.0.0'}, {:wabbit, 'WabbitMQ', '0.0.1'}]}
            ]

    name = "Processes"

    refute capture_io(fn -> TablePrint.print_table(input, :procs, name) end) =~ ~r/nope/

    assert capture_io(fn -> TablePrint.print_table(input, :procs, name) end)
    |> String.split("\n")
    |> length == 5
  end

  test "A result that isn't a list does nothing" do
    input = :invalid
    name = "nope"
    assert capture_io(fn -> TablePrint.print_table(input, :procs, name) end) == ""
  end
end
