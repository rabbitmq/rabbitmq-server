## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule HelpersTest do
  alias RabbitMQ.CLI.Core.{Config, Helpers}
  import RabbitMQ.CLI.Core.{CodePath, Memory}

  use ExUnit.Case, async: false
  import TestHelper

  ## --------------------- get_rabbit_hostname()/0 tests -------------------------

  test "RabbitMQ hostname is properly formed" do
    assert Helpers.get_rabbit_hostname() |> Atom.to_string() =~ ~r/rabbit@\w+/
  end

  ## ------------------- memory_unit* tests --------------------

  test "an invalid memory unit fails " do
    assert memory_unit_absolute(10, "gigantibytes") == {:bad_argument, ["gigantibytes"]}
  end

  test "an invalid number fails " do
    assert memory_unit_absolute("lots", "gigantibytes") ==
             {:bad_argument, ["lots", "gigantibytes"]}

    assert memory_unit_absolute(-1, "gigantibytes") == {:bad_argument, [-1, "gigantibytes"]}
  end

  test "valid number and unit returns a valid result  " do
    assert memory_unit_absolute(10, "k") == 10240
    assert memory_unit_absolute(10, "kiB") == 10240
    assert memory_unit_absolute(10, "M") == 10_485_760
    assert memory_unit_absolute(10, "MiB") == 10_485_760
    assert memory_unit_absolute(10, "G") == 10_737_418_240
    assert memory_unit_absolute(10, "GiB") == 10_737_418_240
    assert memory_unit_absolute(10, "kB") == 10000
    assert memory_unit_absolute(10, "MB") == 10_000_000
    assert memory_unit_absolute(10, "GB") == 10_000_000_000
    assert memory_unit_absolute(10, "") == 10
  end

  ## ------------------- Helpers.normalise_node_option tests --------------------

  test "longnames: 'rabbit' as node name, correct domain is used" do
    default_name = Config.default(:node)
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
    default_name = Config.default(:node)
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

  ## ------------------- redact_uri_credentials/1 tests --------------------

  test "redact_uri_credentials: strips userinfo from a URI" do
    assert Helpers.redact_uri_credentials("amqp://alice:s3cr3t@host1:5672/vhost") ==
             "amqp://host1:5672/vhost"
  end

  test "redact_uri_credentials: strips userinfo from every URI found in a JSON blob" do
    value =
      "{\"src-uri\":\"amqp://alice:s3cr3t@host1\",\"dest-uri\":\"amqp://bob:hunter2@host2\",\"src-queue\":\"q\"}"

    redacted = Helpers.redact_uri_credentials(value)

    refute redacted =~ "s3cr3t"
    refute redacted =~ "hunter2"
    assert redacted =~ "host1"
    assert redacted =~ "host2"
    assert redacted =~ "src-queue"
    assert redacted =~ "\"q\""
  end

  test "redact_uri_credentials: leaves a value without credentials untouched" do
    assert Helpers.redact_uri_credentials("{\"uri\":\"amqp://127.0.0.1:5672\"}") ==
             "{\"uri\":\"amqp://127.0.0.1:5672\"}"
  end

  test "redact_uri_credentials: strips the query string, which can carry a private key passphrase" do
    assert Helpers.redact_uri_credentials(
             "{\"src-uri\":\"amqps://host1?keyfile=%2Fpath%2Fkey.pem&password=s3cr3t\"}"
           ) == "{\"src-uri\":\"amqps://host1\"}"
  end

  test "redact_uri_credentials: strips both the userinfo and the query string of the same URI" do
    assert Helpers.redact_uri_credentials(
             "{\"src-uri\":\"amqps://alice:s3cr3t@host1?password=hunter2\",\"src-queue\":\"q\"}"
           ) == "{\"src-uri\":\"amqps://host1\",\"src-queue\":\"q\"}"
  end

  test "redact_uri_credentials: does not mistake a literal @ in the query string for userinfo" do
    assert Helpers.redact_uri_credentials("amqp://myhost?ssl_options.password=p@ssw0rd") ==
             "amqp://myhost"
  end

  test "redact_uri_credentials: leaves a question mark outside a URI alone" do
    assert Helpers.redact_uri_credentials("{\"note\":\"why? because\",\"max-hops\":1}") ==
             "{\"note\":\"why? because\",\"max-hops\":1}"
  end

  test "redact_uri_credentials: strips the whole userinfo when it contains a literal @, e.g. an email-style username" do
    assert Helpers.redact_uri_credentials("amqp://alice@example.com:s3cr3t@host1:5672/") ==
             "amqp://host1:5672/"
  end

  ## ------------------- require_rabbit/1 tests --------------------

  test "locate plugin with version number in filename" do
    plugins_directory_03 = fixture_plugins_path("plugins-subdirectory-03")
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])
    opts = %{plugins_dir: to_string(plugins_directory_03), rabbitmq_home: rabbitmq_home}

    desc = ~c"A mock RabbitMQ plugin to be used in tests"
    vsn = ~c"0.1.0"

    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_03, desc, vsn}) ==
             false

    require_rabbit_and_plugins(opts)
    Application.load(:mock_rabbitmq_plugins_03)
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_03, desc, vsn})
  end

  test "locate plugin without version number in filename" do
    plugins_directory_04 = fixture_plugins_path("plugins-subdirectory-04")
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])
    opts = %{plugins_dir: to_string(plugins_directory_04), rabbitmq_home: rabbitmq_home}

    desc = ~c"A mock RabbitMQ plugin to be used in tests"
    vsn = ~c"rolling"

    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_04, desc, vsn}) ==
             false

    require_rabbit_and_plugins(opts)
    Application.load(:mock_rabbitmq_plugins_04)
    assert Enum.member?(Application.loaded_applications(), {:mock_rabbitmq_plugins_04, desc, vsn})
  end
end
