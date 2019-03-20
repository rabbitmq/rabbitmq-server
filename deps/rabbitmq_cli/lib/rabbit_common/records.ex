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

defmodule RabbitCommon.Records do
  require Record
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :amqqueue, extract(:amqqueue, from_lib: "rabbit_common/include/rabbit.hrl")
  defrecord :listener, extract(:listener, from_lib: "rabbit_common/include/rabbit.hrl")
  defrecord :plugin, extract(:plugin, from_lib: "rabbit_common/include/rabbit.hrl")
  defrecord :resource, extract(:resource, from_lib: "rabbit_common/include/rabbit.hrl")
end
