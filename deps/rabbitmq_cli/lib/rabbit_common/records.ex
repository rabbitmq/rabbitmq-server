
defmodule RabbitCommon.Records do
  require Record
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :amqqueue, extract(:amqqueue, from_lib: "rabbit_common/include/rabbit.hrl")
end
