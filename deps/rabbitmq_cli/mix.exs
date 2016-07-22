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



defmodule RabbitMQCtl.MixfileBase do
  use Mix.Project

  def project do
    [
      app: :rabbitmqctl,
      version: "0.0.1",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      escript: [main_module: RabbitMQCtl, emu_args: "-hidden"],
      deps: deps
   ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :mix],
     env: [scopes: ['rabbitmq-plugins': :plugins,
                    rabbitmqctl: :ctl]]
    ]
    |> add_modules(Mix.env)
  end

  
  defp add_modules(app, :test) do
    path = Mix.Project.compile_path
    mods = modules_from(Path.wildcard("#{path}/*.beam"))
    test_modules = [RabbitMQ.CLI.Ctl.Commands.DuckCommand,
                    RabbitMQ.CLI.Ctl.Commands.GrayGooseCommand,
                    RabbitMQ.CLI.Ctl.Commands.UglyDucklingCommand,
                    RabbitMQ.CLI.Plugins.Commands.StorkCommand,
                    RabbitMQ.CLI.Plugins.Commands.HeronCommand,
                    RabbitMQ.CLI.Custom.Commands.CrowCommand,
                    RabbitMQ.CLI.Custom.Commands.RavenCommand,
                    RabbitMQ.CLI.Seagull.Commands.SeagullCommand]
    [{:modules, mods ++ test_modules |> Enum.sort} | app]
  end
  defp add_modules(app, _) do
    app
  end

  defp modules_from(beams) do
    Enum.map beams, &(&1 |> Path.basename |> Path.rootname(".beam") |> String.to_atom)
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
    # RabbitMQ components (rabbit_common and amqp_client) requires GNU
    # Make. Let's verify first if GNU Make is available before blindly
    # using "make" and get a Tolstoï-long parsing error message.
    make = find_gnu_make()

    [
      # {
      #   :rabbit,
      #   git: "https://github.com/rabbitmq/rabbitmq-server.git",
      #   branch: "master",
      #   compile: make,
      #   only: :test
      # },
      {
        :rabbit_common,
        git: "https://github.com/rabbitmq/rabbitmq-common.git",
        branch: "master",
        compile: make
      },
      {
        :amqp_client,
        git: "https://github.com/rabbitmq/rabbitmq-erlang-client.git",
        branch: "master",
        compile: make,
        override: true
      },
      {
        :amqp,
        git: "https://github.com/pma/amqp.git",
        branch: "master"
      }
    ]
  end

  defp find_gnu_make do
    possible_makes = [
      System.get_env("MAKE"),
      "make",
      "gmake"
    ]
    test_gnu_make(possible_makes)
  end

  defp test_gnu_make([nil | rest]) do
    test_gnu_make(rest)
  end
  defp test_gnu_make([make | rest]) do
    {output, _} = System.cmd(make, ["--version"], stderr_to_stdout: true)
    case String.contains?(output, "GNU Make") do
      true  -> make
      false -> test_gnu_make(rest)
    end
  end
  defp test_gnu_make([]) do
    nil
  end

end
