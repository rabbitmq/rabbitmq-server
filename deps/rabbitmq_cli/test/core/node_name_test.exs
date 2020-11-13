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
