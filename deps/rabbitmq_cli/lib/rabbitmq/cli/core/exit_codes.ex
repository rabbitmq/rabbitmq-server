## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

# Lists predefined error exit codes used by RabbitMQ CLI tools.
# The codes are adopted from [1], which (according to our team's research)
# is possibly the most standardized set of command line tool exit codes there is.
#
# 1. https://www.freebsd.org/cgi/man.cgi?query=sysexits&apropos=0&sektion=0&manpath=FreeBSD+12.0-RELEASE&arch=default&format=html
defmodule RabbitMQ.CLI.Core.ExitCodes do
  @exit_ok 0
  @exit_usage 64
  @exit_dataerr 65
  @exit_nouser 67
  @exit_unavailable 69
  @exit_software 70
  @exit_tempfail 75
  @exit_config 78

  @type exit_code :: integer

  def exit_ok, do: @exit_ok
  def exit_usage, do: @exit_usage
  def exit_dataerr, do: @exit_dataerr
  def exit_nouser, do: @exit_nouser
  def exit_unavailable, do: @exit_unavailable
  def exit_software, do: @exit_software
  def exit_tempfail, do: @exit_tempfail
  def exit_config, do: @exit_config

  def exit_code_for({:validation_failure, :not_enough_args}), do: exit_usage()
  def exit_code_for({:validation_failure, :too_many_args}), do: exit_usage()
  def exit_code_for({:validation_failure, {:not_enough_args, _}}), do: exit_usage()
  def exit_code_for({:validation_failure, {:too_many_args, _}}), do: exit_usage()
  def exit_code_for({:validation_failure, {:bad_argument, _}}), do: exit_dataerr()
  def exit_code_for({:validation_failure, :bad_argument}), do: exit_dataerr()
  def exit_code_for({:validation_failure, :eperm}), do: exit_dataerr()
  def exit_code_for({:validation_failure, {:bad_option, _}}), do: exit_usage()
  def exit_code_for({:validation_failure, _}), do: exit_usage()
  # a special case of bad_argument
  def exit_code_for({:no_such_vhost, _}), do: exit_dataerr()
  def exit_code_for({:no_such_user, _}), do: exit_nouser()
  def exit_code_for({:badrpc_multi, :timeout, _}), do: exit_tempfail()
  def exit_code_for({:badrpc, :timeout}), do: exit_tempfail()
  def exit_code_for({:badrpc, {:timeout, _}}), do: exit_tempfail()
  def exit_code_for({:badrpc, {:timeout, _, _}}), do: exit_tempfail()
  def exit_code_for(:timeout), do: exit_tempfail()
  def exit_code_for({:timeout, _}), do: exit_tempfail()
  def exit_code_for({:badrpc_multi, :nodedown, _}), do: exit_unavailable()
  def exit_code_for({:badrpc, :nodedown}), do: exit_unavailable()
  def exit_code_for({:node_name, _}), do: exit_dataerr()
  def exit_code_for({:incompatible_version, _, _}), do: exit_unavailable()
  def exit_code_for({:error, _}), do: exit_unavailable()
end
