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


defmodule PluginsFormatterTest do
  use ExUnit.Case, async: false

  @formatter RabbitMQ.CLI.Formatters.Plugins

  test "format_output pending upgrade version message" do
    result = @formatter.format_output(
        %{status: :running,
          plugins: [%{name: :amqp_client, enabled: :implicit, running: true, version: '3.7.0', running_version: nil},
                    %{name: :mock_rabbitmq_plugins_01, enabled: :not_enabled, running: false, version: '0.2.0', running_version: nil},
                    %{name: :mock_rabbitmq_plugins_02, enabled: :enabled, running: true, version: '0.2.0', running_version: '0.1.0'},
                    %{name: :rabbitmq_federation, enabled: :enabled, running: true, version: '3.7.0', running_version: nil},
                    %{name: :rabbitmq_stomp, enabled: :enabled, running: true, version: '3.7.0', running_version: nil}],
          format: :normal}, %{node: "rabbit@localhost"})
    assert result == [" Configured: E = explicitly enabled; e = implicitly enabled",
                     " | Status: * = running on rabbit@localhost", " |/",
                     "[e*] amqp_client              3.7.0", "[  ] mock_rabbitmq_plugins_01 0.2.0",
                     "[E*] mock_rabbitmq_plugins_02 0.1.0 (pending upgrade to 0.2.0)",
                     "[E*] rabbitmq_federation      3.7.0", "[E*] rabbitmq_stomp           3.7.0"]
  end

end
