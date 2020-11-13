## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule NodeNameTest do
  use ExUnit.Case, async: true

  @subject RabbitMQ.CLI.Core.NodeName

  test "shortnames: RabbitMQ nodename is properly formed from atom" do
    want = String.to_atom("rabbit@#{:inet_db.gethostname()}")
    {:ok, got} = @subject.create(:rabbit, :shortnames)
    assert want == got
  end

  test "shortnames: RabbitMQ nodename is properly formed from string" do
    want = String.to_atom("rabbit@#{:inet_db.gethostname()}")
    {:ok, got} = @subject.create("rabbit", :shortnames)
    assert want == got
  end

  test "shortnames: RabbitMQ nodename is properly formed with trailing @" do
    want = String.to_atom("rabbit@#{:inet_db.gethostname()}")
    {:ok, got} = @subject.create(:rabbit@, :shortnames)
    assert want == got
  end

  test "shortnames: RabbitMQ nodename is properly formed with host part" do
    want = :rabbit@foofoo
    {:ok, got} = @subject.create(want, :shortnames)
    assert want == got
  end

  test "shortnames: nodename head only supports alphanumerics, underscores and hyphens in name head" do
    {:error, {:node_name, :invalid_node_name_head}} = @subject.create("кириллица", :shortnames)
  end

  test "longnames: RabbitMQ nodename is properly formed from atom" do
    {:ok, got} = @subject.create(:rabbit, :longnames)
    assert Atom.to_string(got) =~ ~r/rabbit@[\w\-]+\.\w+/
  end

  test "longnames: RabbitMQ nodename is properly formed from string" do
    {:ok, got} = @subject.create("rabbit", :longnames)
    assert Atom.to_string(got) =~ ~r/rabbit@[\w\-]+\.\w+/
  end

  test "longnames: RabbitMQ nodename is properly formed from atom with domain" do
    want = :"rabbit@localhost.localdomain"
    {:ok, got} = @subject.create(want, :longnames)
    assert want == got
  end

  test "longnames: RabbitMQ nodename is properly formed from string with domain" do
    name_str = "rabbit@localhost.localdomain"
    want = String.to_atom(name_str)
    {:ok, got} = @subject.create(name_str, :longnames)
    assert want == got
  end

  test "longnames: RabbitMQ nodename is properly formed from string with partial domain" do
    name_str = "rabbit@localhost"
    want = String.to_atom(name_str <> "." <> @subject.domain())
    {:ok, got} = @subject.create(name_str, :longnames)
    assert want == got
  end

  test "longnames: nodename head only supports alphanumerics, underscores and hyphens in name head" do
    {:error, {:node_name, :invalid_node_name_head}} = @subject.create("кириллица", :longnames)
  end
end
