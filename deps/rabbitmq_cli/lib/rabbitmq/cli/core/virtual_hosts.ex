## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
defmodule RabbitMQ.CLI.Core.VirtualHosts do
  @known_queue_types ["quorum", "stream", "classic"]
  @empty_default_queue_type_message "Default queue type must not be an empty string"

  # Must match ?MAX_VHOST_TAGS in rabbit_vhost.
  @max_vhost_tags 32

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

  def validate_tags(tags) when is_binary(tags) do
    validate_tag_count(String.split(tags, ",", trim: true))
  end

  def validate_tags(_), do: :ok

  def validate_tag_count(tags) when length(tags) > @max_vhost_tags do
    {:validation_failure,
     {:bad_argument,
      "A virtual host can have at most #{@max_vhost_tags} tags (received #{length(tags)})"}}
  end

  def validate_tag_count(_), do: :ok

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
