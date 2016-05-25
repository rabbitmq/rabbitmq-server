## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.



defmodule RabbitMQCtl.Mixfile do
  use Mix.Project

  def project do
    [
      app: :rabbitmqctl,
      version: "0.0.1",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      escript: escript_config,
      deps: deps,
   ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :mix]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {
        :rabbit_common,
        git: "https://github.com/rabbitmq/rabbitmq-common.git",
        branch: "master"
      },
      # for test helper(s) that close connections and so on
      # {
      #   :rabbit,
      #   git: "https://github.com/rabbitmq/rabbitmq-server.git",
      #   branch: "stable"
      # },      
      {
        :amqp_client,
        git: "https://github.com/rabbitmq/rabbitmq-erlang-client.git",
        branch: "master",
        override: true
      },
      {
        :amqp,
        git: "https://github.com/pma/amqp.git",
        branch: "master"
      }
    ]
  end

  defp escript_config do
    [
      main_module: RabbitMQCtl
    ]
  end
end


