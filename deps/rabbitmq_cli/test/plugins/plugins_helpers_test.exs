## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule PluginsHelpersTest do
  use ExUnit.Case, async: false

  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers

  setup_all do
    local_node_str = :rabbit_env.get_context()[:nodename] |> to_string()

    case String.split(local_node_str, "@", parts: 2) do
      [local_name, local_host] ->
        {:ok, local_name: local_name, local_host: local_host}

      _ ->
        {:ok, local_name: local_node_str, local_host: nil}
    end
  end

  describe "node_is_local?/1" do
    test "returns true when opts has no node key" do
      assert PluginHelpers.node_is_local?(%{})
    end

    test "returns true for the configured local node", context do
      local_atom = String.to_atom("#{context[:local_name]}@#{context[:local_host]}")
      assert PluginHelpers.node_is_local?(%{node: local_atom})
    end

    test "returns false for a node on a clearly different host", context do
      name = context[:local_name]
      refute PluginHelpers.node_is_local?(%{node: String.to_atom("#{name}@unknownhost.example.com")})
    end

    test "returns false for a node with a different name on the local host", context do
      if host = context[:local_host] do
        refute PluginHelpers.node_is_local?(%{node: String.to_atom("differentname@#{host}")})
      end
    end

    test "returns false for a node with a different name and different host" do
      refute PluginHelpers.node_is_local?(%{node: :"othername@otherhost.example.com"})
    end

    ## hosts_match?/2 treats a short hostname and its FQDN as the same host.
    test "returns true for a FQDN form of the local host (short-name prefix match)", context do
      if host = context[:local_host] do
        name = context[:local_name]
        fqdn_node = String.to_atom("#{name}@#{host}.localdomain")
        assert PluginHelpers.node_is_local?(%{node: fqdn_node})
      end
    end

    test "returns true when the local host is a FQDN and the node uses the short form", context do
      if host = context[:local_host] do
        name = context[:local_name]
        short_node = String.to_atom("#{name}@#{String.split(host, ".", parts: 2) |> hd()}")

        if String.contains?(host, ".") do
          assert PluginHelpers.node_is_local?(%{node: short_node})
        end
      end
    end

    test "returns true for a bare node name matching the local name", context do
      if context[:local_host] do
        bare_node = String.to_atom(context[:local_name])
        assert PluginHelpers.node_is_local?(%{node: bare_node})
      end
    end

    test "returns false for a bare node name that does not match the local name" do
      refute PluginHelpers.node_is_local?(%{node: :definitelynotthelocalnodename})
    end
  end
end
