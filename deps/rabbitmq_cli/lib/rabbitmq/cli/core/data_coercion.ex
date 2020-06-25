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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

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
