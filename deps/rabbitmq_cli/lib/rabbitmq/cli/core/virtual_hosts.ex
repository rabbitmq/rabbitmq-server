## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
defmodule RabbitMQ.CLI.Core.VirtualHosts do
  @known_queue_types ["quorum", "stream", "classic"]
  @empty_default_queue_type_message "Default queue type must not be an empty string"

  def parse_tags(tags) do
    case tags do
      nil ->
        nil

      val ->
        String.split(val, ",", trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.map(&String.to_atom/1)
    end
  end

  def invalid_queue_type_message(""), do: @empty_default_queue_type_message
  def invalid_queue_type_message(dqt) when is_binary(dqt), do: "#{dqt} is not a valid queue type"
  def invalid_queue_type_message(dqt), do: "#{inspect(dqt)} is not a valid queue type"

  def validate_default_queue_type(opts) do
    case opts[:default_queue_type] do
      nil ->
        :ok

      "" ->
        {:validation_failure, {:bad_argument, @empty_default_queue_type_message}}

      val when val in @known_queue_types ->
        :ok

      other ->
        {:validation_failure,
         {:bad_argument,
          "Default queue type must be one of: #{Enum.join(@known_queue_types, ", ")}. Provided: #{other}"}}
    end
  end
end
