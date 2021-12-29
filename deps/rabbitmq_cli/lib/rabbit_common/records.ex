## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitCommon.Records do
  require Record
  import Record, only: [defrecord: 2, extract: 2]

  # Important: amqqueue records must not be used directly since they are versioned
  #            for mixed version cluster compatibility. Convert records
  #            to maps on the server end to access the fields of those records. MK.
  defrecord :listener, extract(:listener, from_lib: "rabbit_common/include/rabbit.hrl")
  defrecord :plugin, extract(:plugin, from_lib: "rabbit_common/include/rabbit.hrl")
  defrecord :resource, extract(:resource, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :hostent, extract(:hostent, from_lib: "kernel/include/inet.hrl")
end
