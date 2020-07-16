## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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
