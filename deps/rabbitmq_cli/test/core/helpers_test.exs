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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule HelpersTest do
  alias RabbitMQ.CLI.Core.Config

  use ExUnit.Case, async: false
  import TestHelper

  @subject RabbitMQ.CLI.Core.Helpers

  ## --------------------- get_rabbit_hostname()/0 tests -------------------------

  test "RabbitMQ hostname is properly formed" do
    assert @subject.get_rabbit_hostname() |> Atom.to_string =~ ~r/rabbit@\w+/
  end

  ## ------------------- memory_unit* tests --------------------

  test "an invalid memory unit fails " do
    assert @subject.memory_unit_absolute(10, "gigantibytes") == {:bad_argument, ["gigantibytes"]}
  end

  test "an invalid number fails " do
    assert @subject.memory_unit_absolute("lots", "gigantibytes") == {:bad_argument, ["lots", "gigantibytes"]}
    assert @subject.memory_unit_absolute(-1, "gigantibytes") == {:bad_argument, [-1, "gigantibytes"]}
  end

  test "valid number and unit returns a valid result  " do
      assert @subject.memory_unit_absolute(10, "k") == 10240
      assert @subject.memory_unit_absolute(10, "kiB") == 10240
      assert @subject.memory_unit_absolute(10, "M") == 10485760
      assert @subject.memory_unit_absolute(10, "MiB") == 10485760
      assert @subject.memory_unit_absolute(10, "G") == 10737418240
      assert @subject.memory_unit_absolute(10, "GiB")== 10737418240
      assert @subject.memory_unit_absolute(10, "kB")== 10000
      assert @subject.memory_unit_absolute(10, "MB")== 10000000
      assert @subject.memory_unit_absolute(10, "GB")== 10000000000
      assert @subject.memory_unit_absolute(10, "")  == 10
  end

  ## ------------------- normalise_node_option tests --------------------

  test "longnames: 'rabbit' as node name, correct domain is used" do
    default_name = Config.get_option(:node)
    options = %{node: default_name, longnames: true}
    options = @subject.normalise_node_option(options)
    assert options[:node] == :"rabbit@#{hostname()}.#{domain()}"
  end

  test "shortnames: 'rabbit' as node name, no domain is used" do
    options = %{node: :rabbit, longnames: false}
    options = @subject.normalise_node_option(options)
    assert options[:node] == :"rabbit@#{hostname()}"
  end

  ## ------------------- normalise_node tests (:shortnames) --------------------

  test "shortnames: if nil input, retrieve standard rabbit hostname" do
    assert @subject.normalise_node(nil) == get_rabbit_hostname()
  end

  test "shortnames: if input is an atom short name, return the atom with hostname" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = @subject.normalise_node(:rabbit_test)
    assert want == got
  end

  test "shortnames: if input is a string fully qualified node name, return an atom" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = @subject.normalise_node("rabbit_test@#{hostname()}")
    assert want == got
  end

  test "shortnames: if input is a short node name, host name is added" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = @subject.normalise_node("rabbit_test")
    assert want == got
  end

  test "shortnames: if input is a hostname without a node name, default node name is added" do
    default_name = Config.get_option(:node)
    want = String.to_atom("#{default_name}@#{hostname()}")
    got = @subject.normalise_node("@#{hostname()}")
    assert want == got
  end

  test "shortnames: if input is a short node name with an @ and no hostname, local host name is added" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = @subject.normalise_node("rabbit_test@")
    assert want == got
  end

  test "shortnames: if input contains more than one @, return an atom" do
    want = String.to_atom("rabbit@rabbit_test@#{hostname()}")
    got = @subject.normalise_node("rabbit@rabbit_test@#{hostname()}")
    assert want == got
  end

  ## ------------------- normalise_node tests (:longnames) --------------------

  test "longnames: if nil input, retrieve standard rabbit hostname" do
    want = get_rabbit_hostname(:longnames)
    got = @subject.normalise_node(nil, :longnames)
    assert want == got
  end

  test "longnames: if input is an atom short name, return the atom with full hostname" do
    want = String.to_atom("rabbit_test@#{hostname()}.#{domain()}")
    got = @subject.normalise_node(:rabbit_test, :longnames)
    assert want == got
  end

  ## ------------------- require_rabbit/1 tests --------------------

  test "locate plugin with version number in filename" do
    plugins_directory_03 = fixture_plugins_path("plugins-subdirectory-03")
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])
    opts = %{plugins_dir: to_string(plugins_directory_03),
             rabbitmq_home: rabbitmq_home}
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_03, 'New project', '0.1.0'}) == false
    @subject.require_rabbit_and_plugins(opts)
    Application.load(:mock_rabbitmq_plugins_03)
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_03, 'New project', '0.1.0'})
  end

  test "locate plugin without version number in filename" do
    plugins_directory_04 = fixture_plugins_path("plugins-subdirectory-04")
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])
    opts = %{plugins_dir: to_string(plugins_directory_04),
             rabbitmq_home: rabbitmq_home}
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_04, 'New project', 'rolling'}) == false
    @subject.require_rabbit_and_plugins(opts)
    Application.load(:mock_rabbitmq_plugins_04)
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_04, 'New project', 'rolling'})
  end

end
