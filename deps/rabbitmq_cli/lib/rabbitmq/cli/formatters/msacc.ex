## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Formatters.Msacc do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(output, _) do
    {:ok, io} = StringIO.open("")
    :msacc.print(io, output, %{})
    StringIO.flush(io)
  end

  def format_stream(stream, options) do
    [format_output(Enum.to_list(stream), options)]
  end
end
