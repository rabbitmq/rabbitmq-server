## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Core.Users do
  # Defined here to not drag in rabbit.hrl and Erlang compilation in an Elixir
  # sub-project
  @internal_user "rmq-internal"
  @cli_user "cli-user"

  def internal_user do
    @internal_user
  end

  def cli_user do
    @cli_user
  end
end