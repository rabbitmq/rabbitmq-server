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

defmodule HelpersTest do
  alias RabbitMQ.CLI.Core.{Config, Helpers}
  import RabbitMQ.CLI.Core.{CodePath, Memory}

  use ExUnit.Case, async: false
  import TestHelper

  ## --------------------- get_rabbit_hostname()/0 tests -------------------------

  test "RabbitMQ hostname is properly formed" do
    assert Helpers.get_rabbit_hostname() |> Atom.to_string =~ ~r/rabbit@\w+/
  end

  ## ------------------- memory_unit* tests --------------------

  test "an invalid memory unit fails " do
    assert memory_unit_absolute(10, "gigantibytes") == {:bad_argument, ["gigantibytes"]}
  end

  test "an invalid number fails " do
    assert memory_unit_absolute("lots", "gigantibytes") == {:bad_argument, ["lots", "gigantibytes"]}
    assert memory_unit_absolute(-1, "gigantibytes") == {:bad_argument, [-1, "gigantibytes"]}
  end

  test "valid number and unit returns a valid result  " do
      assert memory_unit_absolute(10, "k") == 10240
      assert memory_unit_absolute(10, "kiB") == 10240
      assert memory_unit_absolute(10, "M") == 10485760
      assert memory_unit_absolute(10, "MiB") == 10485760
      assert memory_unit_absolute(10, "G") == 10737418240
      assert memory_unit_absolute(10, "GiB")== 10737418240
      assert memory_unit_absolute(10, "kB")== 10000
      assert memory_unit_absolute(10, "MB")== 10000000
      assert memory_unit_absolute(10, "GB")== 10000000000
      assert memory_unit_absolute(10, "")  == 10
  end

  ## ------------------- Helpers.normalise_node_option tests --------------------

  test "longnames: 'rabbit' as node name, correct domain is used" do
    default_name = Config.get_option(:node)
    options = %{node: default_name, longnames: true}
    {:ok, options} = Helpers.normalise_node_option(options)
    assert options[:node] == :"rabbit@#{hostname()}.#{domain()}"
  end

  test "shortnames: 'rabbit' as node name, no domain is used" do
    options = %{node: :rabbit, longnames: false}
    {:ok, options} = Helpers.normalise_node_option(options)
    assert options[:node] == :"rabbit@#{hostname()}"
  end

  ## ------------------- normalise_node tests (:shortnames) --------------------

  test "shortnames: if nil input, retrieve standard rabbit hostname" do
    assert Helpers.normalise_node(nil, :shortnames) == get_rabbit_hostname()
  end

  test "shortnames: if input is an atom short name, return the atom with hostname" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = Helpers.normalise_node(:rabbit_test, :shortnames)
    assert want == got
  end

  test "shortnames: if input is a string fully qualified node name, return an atom" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = Helpers.normalise_node("rabbit_test@#{hostname()}", :shortnames)
    assert want == got
  end

  test "shortnames: if input is a short node name, host name is added" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = Helpers.normalise_node("rabbit_test", :shortnames)
    assert want == got
  end

  test "shortnames: if input is a hostname without a node name, default node name is added" do
    default_name = Config.get_option(:node)
    want = String.to_atom("#{default_name}@#{hostname()}")
    got = Helpers.normalise_node("@#{hostname()}", :shortnames)
    assert want == got
  end

  test "shortnames: if input is a short node name with an @ and no hostname, local host name is added" do
    want = String.to_atom("rabbit_test@#{hostname()}")
    got = Helpers.normalise_node("rabbit_test@", :shortnames)
    assert want == got
  end

  test "shortnames: if input contains more than one @, return an atom" do
    want = String.to_atom("rabbit@rabbit_test@#{hostname()}")
    got = Helpers.normalise_node("rabbit@rabbit_test@#{hostname()}", :shortnames)
    assert want == got
  end

  ## ------------------- normalise_node tests (:longnames) --------------------

  test "longnames: if nil input, retrieve standard rabbit hostname" do
    want = get_rabbit_hostname(:longnames)
    got = Helpers.normalise_node(nil, :longnames)
    assert want == got
  end

  test "longnames: if input is an atom short name, return the atom with full hostname" do
    want = String.to_atom("rabbit_test@#{hostname()}.#{domain()}")
    got = Helpers.normalise_node(:rabbit_test, :longnames)
    assert want == got
  end

  ## ------------------- require_rabbit/1 tests --------------------

  test "locate plugin with version number in filename" do
    plugins_directory_03 = fixture_plugins_path("plugins-subdirectory-03")
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])
    opts = %{plugins_dir: to_string(plugins_directory_03),
             rabbitmq_home: rabbitmq_home}
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_03, 'New project', '0.1.0'}) == false
    require_rabbit_and_plugins(opts)
    Application.load(:mock_rabbitmq_plugins_03)
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_03, 'New project', '0.1.0'})
  end

  test "locate plugin without version number in filename" do
    plugins_directory_04 = fixture_plugins_path("plugins-subdirectory-04")
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])
    opts = %{plugins_dir: to_string(plugins_directory_04),
             rabbitmq_home: rabbitmq_home}
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_04, 'New project', 'rolling'}) == false
    require_rabbit_and_plugins(opts)
    Application.load(:mock_rabbitmq_plugins_04)
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_04, 'New project', 'rolling'})
  end

end
