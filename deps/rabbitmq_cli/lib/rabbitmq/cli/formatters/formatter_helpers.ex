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

defmodule RabbitMQ.CLI.Formatters.FormatterHelpers do
  use Bitwise

  def prettify_amqp_table(table, escaped) do
    for {k, t, v} <- table do
      {escape(k, escaped), prettify_typed_amqp_value(t, v, escaped)}
    end
  end

  defp prettify_typed_amqp_value(:longstr, value, escaped) do
    escape(value, escaped)
  end
  defp prettify_typed_amqp_value(:table, value, escaped) do
    prettify_amqp_table(value, escaped)
  end
  defp prettify_typed_amqp_value(:array, value, escaped) do
    for {t, v} <- value, do: prettify_typed_amqp_value(t, v, escaped)
  end
  defp prettify_typed_amqp_value(_type, value, _escaped) do
    value
  end


  def escape(atom, escaped) when is_atom(atom) do
    escape(to_charlist(atom), escaped)
  end
  def escape(bin, escaped)  when is_binary(bin) do
    escape(to_charlist(bin), escaped)
  end
  def escape(l, false) when is_list(l) do
    escape_char(:lists.reverse(l), [])
  end
  def escape(l, true) when is_list(l) do
    l
  end

  defp escape_char([?\\ | t], acc) do
    escape_char(t, [?\\, ?\\ | acc])
  end
  defp escape_char([x | t], acc) when x >= 32 and x != 127 do
    escape_char(t, [x | acc])
  end
  defp escape_char([x | t], acc) do
    escape_char(t, [?\\, ?0 + (x >>> 6), ?0 + (x &&& 0o070 >>> 3),
                    ?0 + (x &&& 7) | acc])
  end
  defp escape_char([], acc) do
    acc
  end
end