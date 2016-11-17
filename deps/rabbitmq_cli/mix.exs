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
    deps_dir = case System.get_env("DEPS_DIR") do
      nil -> "deps"
      dir -> dir
    end
    [
      app: :rabbitmqctl,
      version: "0.0.1",
      elixir: "~> 1.3",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      escript: [main_module: RabbitMQCtl,
                emu_args: "-hidden",
                path: "escript/rabbitmqctl"],
      deps_path: deps_dir,
      deps: deps(deps_dir)
   ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger],
     env: [scopes: ['rabbitmq-plugins': :plugins,
                    rabbitmqctl: :ctl,
                    'rabbitmq-diagnostics': :diagnostics]]
    ]
    |> add_modules(Mix.env)
  end


  defp add_modules(app, :test) do
    # There are issue with building a package without this line ¯\_(ツ)_/¯
    Mix.Project.get
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
  defp deps(deps_dir) do
    # RabbitMQ components (rabbit_common and amqp_client) require GNU
    # Make. This ensures that GNU Make is available before we attempt
    # to use it.
    make = find_gnu_make()

    [
      # {
      #   :rabbit,
      #   path: Path.join(deps_dir, "rabbit"),
      #   compile: make,
      #   override: true
      # },
      {
        :rabbit_common,
        path: Path.join(deps_dir, "rabbit_common"),
        compile: make,
        override: true
      },
      {
        :amqp_client,
        only: :test,
        path: Path.join(deps_dir, "amqp_client"),
        compile: make,
        override: true
      },
      {
        :amqp, "~> 0.1.5"
        only: :test,
        path: Path.join(deps_dir, "amqp")
      },
      {
        :json, "~> 1.0.0",
        path: Path.join(deps_dir, "json")
      },
      {
        :csv, "~> 1.4.2",
        path: Path.join(deps_dir, "csv")
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
