## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.FeatureFlags do

  #
  # API
  #

  def feature_flag_lines(feature_flags) do
    feature_flags
    |> Enum.map(fn %{name: name, state: state} ->
      "Flag: #{name}, state: #{state}"
    end)
  end
end
