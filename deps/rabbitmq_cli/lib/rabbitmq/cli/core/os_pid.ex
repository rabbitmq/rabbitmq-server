## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.OsPid do
  @external_process_check_interval 1000

  @pid_regex ~r/^\s*(?<pid>\d+)/

  #
  # API
  #

  def wait_for_os_process_death(pid) do
    case :rabbit_misc.is_os_process_alive(pid) do
      true ->
        :timer.sleep(@external_process_check_interval)
        wait_for_os_process_death(pid)

      false ->
        :ok
    end
  end

  def read_pid_from_file(pidfile_path, should_wait) do
    case {:file.read_file(pidfile_path), should_wait} do
      {{:ok, contents}, _} ->
        pid_regex = Regex.recompile!(@pid_regex)

        case Regex.named_captures(pid_regex, contents)["pid"] do
          # e.g. the file is empty
          nil ->
            {:error, :could_not_read_pid_from_file, {:contents, contents}}

          pid_string ->
            try do
              {pid, _remainder} = Integer.parse(pid_string)
              pid
            rescue
              _e in ArgumentError ->
                {:error, {:could_not_read_pid_from_file, {:contents, contents}}}
            end
        end

      # file does not exist, wait and re-check
      {{:error, :enoent}, true} ->
        :timer.sleep(@external_process_check_interval)
        read_pid_from_file(pidfile_path, should_wait)

      {{:error, details}, _} ->
        {:error, :could_not_read_pid_from_file, details}
    end
  end
end
