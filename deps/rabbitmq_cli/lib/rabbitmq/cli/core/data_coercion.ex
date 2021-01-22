## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defprotocol RabbitMQ.CLI.Core.DataCoercion do
  def to_atom(data)
end

defimpl RabbitMQ.CLI.Core.DataCoercion, for: Atom do
  def to_atom(atom), do: atom
end

defimpl RabbitMQ.CLI.Core.DataCoercion, for: BitString do
  def to_atom(string), do: String.to_atom(string)
end

defimpl RabbitMQ.CLI.Core.DataCoercion, for: List do
  def to_atom(list), do: List.to_atom(list)
end
