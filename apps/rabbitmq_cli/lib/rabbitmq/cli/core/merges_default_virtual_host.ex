## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

# Should be used by commands that require rabbit app to be stopped
# but need no other execution environment validators.
defmodule RabbitMQ.CLI.Core.MergesDefaultVirtualHost do
  defmacro __using__(_) do
    quote do
      def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}
    end
  end
end
