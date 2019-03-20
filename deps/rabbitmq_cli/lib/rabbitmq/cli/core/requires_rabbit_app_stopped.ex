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

# Should be used by commands that require rabbit app to be stopped
# but need no other execution environment validators.
defmodule RabbitMQ.CLI.Core.RequiresRabbitAppStopped do
  defmacro __using__(_) do
    quote do
      def validate_execution_environment(args, opts) do
        RabbitMQ.CLI.Core.Validators.rabbit_is_not_running(args, opts)
      end
    end
  end
end
