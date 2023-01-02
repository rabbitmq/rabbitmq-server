## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.FeatureFlags do
  alias RabbitMQ.CLI.Core.ExitCodes

  #
  # API
  #

  def is_enabled_remotely(node_name, feature_flag) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_feature_flags, :is_enabled, [feature_flag]) do
      true -> true
      false -> false
      {:error, _} = error -> error
    end
  end

  def assert_feature_flag_enabled(node_name, feature_flag, success_fun) do
    case is_enabled_remotely(node_name, feature_flag) do
      true ->
        success_fun.()

      false ->
        {:error, ExitCodes.exit_dataerr(),
         "The #{feature_flag} feature flag is not enabled on the target node"}

      {:error, _} = error ->
        error
    end
  end

  def feature_flag_lines(feature_flags) do
    feature_flags
    |> Enum.map(fn %{name: name, state: state} ->
      "Flag: #{name}, state: #{state}"
    end)
  end
end
