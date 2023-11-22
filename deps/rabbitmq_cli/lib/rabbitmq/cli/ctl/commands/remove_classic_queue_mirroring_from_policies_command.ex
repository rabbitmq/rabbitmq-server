## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.RemoveClassicQueueMirroringFromPoliciesCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_mirror_queue_misc,
      :remove_classic_queue_mirroring_from_policies_for_cli,
      [],
      timeout
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "remove_classic_queue_mirroring_from_policies"

  def usage_doc_guides() do
    [
      DocGuide.mirroring()
    ]
  end

  def help_section(), do: :operations

  def description,
    do: "Removes keys that enable classic queue mirroring from all regular and operator policies"

  def banner([], %{}),
    do:
      "Will remove keys that enable classic queue mirroring from all regular and operator policies"
end
