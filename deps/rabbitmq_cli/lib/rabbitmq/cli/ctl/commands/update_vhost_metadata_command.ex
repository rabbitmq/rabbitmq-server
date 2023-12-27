## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.UpdateVhostMetadataCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers, VirtualHosts}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @metadata_keys [:description, :tags, :default_queue_type]

  def switches(), do: [description: :string, tags: :string, default_queue_type: :string]
  def aliases(), do: [d: :description]

  def merge_defaults(args, opts) do
    {args, opts}
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def validate(args, _) when length(args) == 0 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([_vhost], opts) do
    m = :maps.with(@metadata_keys, opts)

    case map_size(m) do
      0 ->
        {:validation_failure, :not_enough_args}

      _ ->
        # description and tags can be anything but default queue type must
        # be a value from a known set
        case m[:default_queue_type] do
          nil ->
            :ok

          "quorum" ->
            :ok

          "stream" ->
            :ok

          "classic" ->
            :ok

          other ->
            {:validation_failure,
             {:bad_argument,
              "Default queue type must be one of: quorum, stream, classic. Provided: #{other}"}}
        end
    end
  end

  def validate(_, _), do: :ok

  def run([vhost], %{node: node_name} = opts) do
    meta = :maps.with(@metadata_keys, opts)
    tags = meta[:tags]

    meta =
      case tags do
        nil -> meta
        other -> %{meta | tags: VirtualHosts.parse_tags(other)}
      end

    :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :update_metadata, [
      vhost,
      meta,
      Helpers.cli_acting_user()
    ])
  end

  def output({:error, :invalid_queue_type}, _opts) do
    {:error, ExitCodes.exit_usage(), "Unsupported default queue type"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage,
    do:
      "update_vhost_metadata <vhost> [--description <description>] [--tags \"<tag1>,<tag2>,<...>\"] [--default-queue-type <quorum|classic|stream>]"

  def usage_additional() do
    [
      ["<vhost>", "Virtual host name"],
      ["--description <description>", "Virtual host description"],
      ["--tags <tag1,tag2>", "Comma-separated list of tags"],
      [
        "--default-queue-type <quorum|classic|stream>",
        "Queue type to use if no type is explicitly provided by the client"
      ]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Updates metadata (tags, description, default queue type) a virtual host"

  def banner([vhost], _), do: "Updating metadata of vhost \"#{vhost}\" ..."
end
