## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


defmodule OsPidTest do
  use ExUnit.Case, async: false
  import TestHelper

  @subject RabbitMQ.CLI.Core.OsPid

  #
  # Tests
  #

  describe "#read_pid_from_file with should_wait = false" do
    test "with a valid pid file returns an integer value" do
      path = fixture_file_path("valid_pidfile.pid")

      assert (File.exists?(path) and File.regular?(path))
      assert @subject.read_pid_from_file(path, false) == 13566
    end

    test "with a valid pid file that includes spaces returns an integer value" do
      path = fixture_file_path("valid_pidfile_with_spaces.pid")

      assert (File.exists?(path) and File.regular?(path))
      assert @subject.read_pid_from_file(path, false) == 83777
    end

    test "with an empty file" do
      path = fixture_file_path("empty_pidfile.pid")

      assert (File.exists?(path) and File.regular?(path))
      assert match?({:error, :could_not_read_pid_from_file, _}, @subject.read_pid_from_file(path, false))
    end

    test "with a non-empty file full of garbage (that doesn't parse)" do
      path = fixture_file_path("invalid_pidfile.pid")

      assert (File.exists?(path) and File.regular?(path))
      assert match?({:error, :could_not_read_pid_from_file, _}, @subject.read_pid_from_file(path, false))
    end

    test "with a file that does not exist" do
      path = fixture_file_path("pidfile_that_does_not_exist_128787df8s7f8%4&^.pid")

      assert !File.exists?(path)
      assert match?({:error, :could_not_read_pid_from_file, _}, @subject.read_pid_from_file(path, false))
    end
  end
end
