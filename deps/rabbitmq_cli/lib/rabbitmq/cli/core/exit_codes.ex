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

# Lists predefined error exit codes used by RabbitMQ CLI tools.
# The codes are adopted from [1], which (according to our team's research)
# is possibly the most standardized set of codes there is.
#
# 1. https://www.freebsd.org/cgi/man.cgi?query=sysexits&apropos=0&sektion=0&manpath=FreeBSD+12.0-RELEASE&arch=default&format=html
defmodule RabbitMQ.CLI.Core.ExitCodes do
  @exit_ok 0
  @exit_usage 64
  @exit_dataerr 65
  @exit_unavailable 69
  @exit_software 70
  @exit_tempfail 75
  @exit_config 78

  @type exit_code :: integer

  def exit_ok, do: @exit_ok
  def exit_usage, do: @exit_usage
  def exit_dataerr, do: @exit_dataerr
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
  def exit_code_for({:badrpc_multi, :timeout, _}), do: exit_tempfail()
  def exit_code_for({:badrpc, :timeout}), do: exit_tempfail()
  def exit_code_for({:badrpc, {:timeout, _}}), do: exit_tempfail()
  def exit_code_for({:badrpc, {:timeout, _, _}}), do: exit_tempfail()
  def exit_code_for(:timeout), do: exit_tempfail()
  def exit_code_for({:timeout, _}), do: exit_tempfail()
  def exit_code_for({:badrpc_multi, :nodedown, _}), do: exit_unavailable()
  def exit_code_for({:badrpc, :nodedown}), do: exit_unavailable()
  def exit_code_for({:node_name, _}), do: exit_dataerr()
  def exit_code_for({:error, _}), do: exit_unavailable()
end
