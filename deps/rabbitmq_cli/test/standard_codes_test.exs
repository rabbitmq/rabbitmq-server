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


defmodule StandardCodesTest do
  use ExUnit.Case, async: false

  test "An :ok is unchanged" do
    assert_unchanged :ok
    assert_unchanged {:ok, "input"}
  end

  test "A node_down is unchanged" do
    assert_unchanged {:badrpc, :nodedown}
  end

  test "A time_out is unchanged" do
    assert_unchanged {:badrpc, :timeout}
  end

  test "A refused is unchanged" do
    assert_unchanged {:refused, "name", "string", []}
  end

  test "A bad argument is unchanged" do
    assert_unchanged {:bad_argument, "insanelybadargument"}
  end

  test "A 'too many arguments' message is unchanged" do
    assert_unchanged {:too_many_args, ["way", "too", "many"]}
  end

  test "A 'not enough arguments' message is unchanged" do
    assert_unchanged {:not_enough_args, [""]}
  end

  test "A properly-tagged error is unchanged" do
    assert_unchanged {:error, {:reason, "excuse"}}
  end

  test "An unidentified atom is converted to an error" do
    assert_error_wrapped :unknown_symbol
  end

  test "A tuple beginning with an unidentified atom is converted to an error" do
    assert_error_wrapped {:error_string, "explanation"}
  end

  test "A data return is converted to {:ok, <data>}" do
    assert StandardCodes.map_to_standard_code(["sandwich"]) == {:ok, ["sandwich"]}
  end


  defp assert_unchanged(input) do
    assert StandardCodes.map_to_standard_code(input) == input
  end

  defp assert_error_wrapped(input) do
    assert StandardCodes.map_to_standard_code(input) == {:error, input}
  end
end
