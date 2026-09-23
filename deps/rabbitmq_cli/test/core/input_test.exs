## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule InputTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO

  @subject RabbitMQ.CLI.Core.Input

  #
  # Tests
  #

  describe "#consume_single_line_string_with_prompt" do
    test "reads and trims a value from stdin, printing the prompt" do
      {value, output} =
        with_io("a value\n", fn ->
          @subject.consume_single_line_string_with_prompt("Value: ", %{})
        end)

      assert value == "a value"
      assert output == "Value: \n"
    end

    test "does not print the prompt when output is silenced" do
      {value, output} =
        with_io("a value\n", fn ->
          @subject.consume_single_line_string_with_prompt("Value: ", %{silent: true})
        end)

      assert value == "a value"
      assert output == ""
    end

    test "returns an empty string for a blank line" do
      assert with_io("\n", fn ->
               @subject.consume_single_line_string_with_prompt("Value: ", %{})
             end)
             |> elem(0) == ""
    end

    test "returns :eof on end of input" do
      assert with_io("", fn ->
               @subject.consume_single_line_string_with_prompt("Value: ", %{})
             end)
             |> elem(0) == :eof
    end
  end

  describe "#infer_password" do
    # These exercise the plain-read fallback: io:get_password/0 bypasses the group leader CaptureIO substitutes.
    test "reads and trims a password from stdin, printing the prompt" do
      {password, output} =
        with_io("a-secret\n", fn ->
          @subject.infer_password("Password: ", %{})
        end)

      assert password == "a-secret"
      assert output == "Password: \n"
    end

    test "does not print the prompt when output is silenced" do
      {password, output} =
        with_io("a-secret\n", fn ->
          @subject.infer_password("Password: ", %{silent: true})
        end)

      assert password == "a-secret"
      assert output == ""
    end

    test "returns an empty string for a blank line" do
      assert with_io("\n", fn ->
               @subject.infer_password("Password: ", %{})
             end)
             |> elem(0) == ""
    end

    test "returns :eof on end of input" do
      assert with_io("", fn ->
               @subject.infer_password("Password: ", %{})
             end)
             |> elem(0) == :eof
    end

    test "does not warn about masking on stderr outside of a real terminal" do
      {_, stderr_output} =
        with_io(:stderr, fn ->
          with_io("a-secret\n", fn ->
            @subject.infer_password("Password: ", %{})
          end)
        end)

      assert stderr_output == ""
    end
  end

  describe "#normalize_line" do
    # CaptureIO substitutes the group leader, which io:get_password/0 bypasses, so hit normalize_line directly here.
    test "passes a binary through" do
      assert @subject.normalize_line("a-secret") == "a-secret"
    end

    test "converts a charlist to a string" do
      assert @subject.normalize_line(~c"a-secret") == "a-secret"
    end

    test "treats a pre-stripped empty charlist as an empty string, not :eof" do
      assert @subject.normalize_line(~c"") == ""
    end

    test "treats a pre-stripped empty binary as an empty string, not :eof" do
      assert @subject.normalize_line("") == ""
    end

    test "treats an io error the same as :eof" do
      assert @subject.normalize_line({:error, :enotsup}) == :eof
    end

    test "passes :eof through" do
      assert @subject.normalize_line(:eof) == :eof
    end
  end
end
