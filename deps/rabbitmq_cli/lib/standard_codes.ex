## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule StandardCodes do

  def map_to_standard_code(:ok), do: :ok
  def map_to_standard_code({:ok, _} = input), do: input
  def map_to_standard_code({:badrpc, :nodedown} = input), do: input
  def map_to_standard_code({:badrpc, :timeout} = input), do: input
  def map_to_standard_code({:refused, _, _, _} = input), do: input
  def map_to_standard_code({:bad_option, _} = input), do: input
  def map_to_standard_code({:bad_argument, _} = input), do: input
  def map_to_standard_code({:too_many_args, _} = input), do: input
  def map_to_standard_code({:not_enough_args, _} = input), do: input
  def map_to_standard_code({:error, _} = input), do: input
  def map_to_standard_code(unknown) when is_atom(unknown), do: {:error, unknown}
  def map_to_standard_code({unknown, _} = input) when is_atom(unknown), do: {:error, input}
  def map_to_standard_code(result) when not is_atom(result), do: {:ok, result}
end
