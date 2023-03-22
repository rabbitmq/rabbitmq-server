## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2023 VMware, Inc. or its affiliates.  All rights reserved.

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
  Macros.defguide("channels")
  Macros.defguide("cli")
  Macros.defguide("clustering")
  Macros.defguide("cluster_formation")
  Macros.defguide("connections")
  Macros.defguide("configuration", path_segment: "configure")
  Macros.defguide("consumers")
  Macros.defguide("definitions")
  Macros.defguide("erlang_versions", path_segment: "which-erlang")
  Macros.defguide("feature_flags")
  Macros.defguide("firehose")
  Macros.defguide("mirroring", path_segment: "ha")
  Macros.defguide("logging")
  Macros.defguide("management")
  Macros.defguide("memory_use")
  Macros.defguide("monitoring")
  Macros.defguide("networking")
  Macros.defguide("parameters")
  Macros.defguide("plugins")
  Macros.defguide("prometheus")
  Macros.defguide("publishers")
  Macros.defguide("queues")
  Macros.defguide("quorum_queues")
  Macros.defguide("stream_plugin", path_segment: "stream")
  Macros.defguide("streams")
  Macros.defguide("runtime_tuning", path_segment: "runtime")
  Macros.defguide("tls", path_segment: "ssl")
  Macros.defguide("troubleshooting")
  Macros.defguide("virtual_hosts", path_segment: "vhosts")
  Macros.defguide("upgrade")
end
