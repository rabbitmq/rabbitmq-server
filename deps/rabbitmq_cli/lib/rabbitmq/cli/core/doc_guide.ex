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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Core.DocGuide.Macros do
  @moduledoc """
  Helper module that works around a compiler limitation: macros cannot
  be used in a module that defines them.
  """
  @default_domain "rabbitmq.com"

  defmacro defguide(name, opts \\ []) do
    domain = Keyword.get(opts, :domain, @default_domain)
    fn_name = String.to_atom(name)
    path_segment = Keyword.get(opts, :path_segment, String.replace(name, "_", "-"))

    quote do
      def unquote(fn_name)() do
        unquote("https://#{domain}/#{path_segment}.html")
      end
    end
  end
end

defmodule RabbitMQ.CLI.Core.DocGuide do
  require RabbitMQ.CLI.Core.DocGuide.Macros
  alias RabbitMQ.CLI.Core.DocGuide.Macros

  #
  # API
  #

  Macros.defguide("access_control")
  Macros.defguide("alarms")
  Macros.defguide("disk_alarms")
  Macros.defguide("alternate_exchange", path_segment: "ae")
  Macros.defguide("cli")
  Macros.defguide("channels")
  Macros.defguide("clustering")
  Macros.defguide("cluster_formation")
  Macros.defguide("connections")
  Macros.defguide("configuration", path_segment: "configure")
  Macros.defguide("consumers")
  Macros.defguide("erlang_versions", path_segment: "which-erlang")
  Macros.defguide("feature_flags", domain: "next.rabbitmq.com")
  Macros.defguide("firehose")
  Macros.defguide("mirroring", path_segment: "ha")
  Macros.defguide("logging")
  Macros.defguide("management")
  Macros.defguide("memory_use")
  Macros.defguide("monitoring")
  Macros.defguide("parameters")
  Macros.defguide("plugins")
  Macros.defguide("queues")
  Macros.defguide("quorum_queues", domain: "next.rabbitmq.com")
  Macros.defguide("runtime_tuning", path_segment: "runtime")
  Macros.defguide("tls", path_segment: "ssl")
  Macros.defguide("troubleshooting")
  Macros.defguide("virtual_hosts", path_segments: "vhosts")
end
