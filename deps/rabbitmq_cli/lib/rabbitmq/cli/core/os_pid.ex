## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

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
