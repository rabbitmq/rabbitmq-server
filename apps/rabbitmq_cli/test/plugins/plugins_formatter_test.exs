## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule PluginsFormatterTest do
  use ExUnit.Case, async: false

  @formatter RabbitMQ.CLI.Formatters.Plugins

  test "format_output with --silent and --minimal" do
    result = @formatter.format_output(
        %{status: :running,
          plugins: [%{name: :amqp_client, enabled: :implicit, running: true, version: '3.7.0', running_version: nil},
                    %{name: :mock_rabbitmq_plugins_01, enabled: :not_enabled, running: false, version: '0.2.0', running_version: nil},
                    %{name: :mock_rabbitmq_plugins_02, enabled: :enabled, running: true, version: '0.2.0', running_version: '0.1.0'},
                    %{name: :rabbitmq_federation, enabled: :enabled, running: true, version: '3.7.0', running_version: nil},
                    %{name: :rabbitmq_stomp, enabled: :enabled, running: true, version: '3.7.0', running_version: nil}],
          format: :minimal}, %{node: "rabbit@localhost", silent: true})
    assert result == ["amqp_client",
                      "mock_rabbitmq_plugins_01",
                      "mock_rabbitmq_plugins_02",
                      "rabbitmq_federation",
                      "rabbitmq_stomp"]
  end

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
