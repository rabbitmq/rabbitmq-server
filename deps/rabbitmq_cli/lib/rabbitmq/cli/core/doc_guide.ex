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
  @root "https://rabbitmq.com"

  defmacro defguide(name) do
    quote do
      def unquote(String.to_atom(name))(), do: unquote("#{@root}/#{name}.html")
    end
  end
  defmacro defguide(name, path_segment) do
    quote do
      def unquote(String.to_atom(name))(), do: unquote("#{@root}/#{path_segment}.html")
    end
  end
end

defmodule RabbitMQ.CLI.Core.DocGuide do
  require RabbitMQ.CLI.Core.DocGuide.Macros
  alias RabbitMQ.CLI.Core.DocGuide.Macros

  #
  # API
  #

  Macros.defguide("alternate_exchange", "ae")
  Macros.defguide("consumers")
  Macros.defguide("parameters")
  Macros.defguide("queues")
end
