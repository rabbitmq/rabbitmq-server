## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQCtl.MixfileBase do
  use Mix.Project

  def project do
    [
      app: :rabbitmqctl,
      version: "3.8.0-dev",
      elixir: ">= 1.10.4 and < 1.13.0",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      escript: [main_module: RabbitMQCtl,
                emu_args: "-hidden",
                path: "escript/rabbitmqctl"],
      deps: deps(),
      aliases: aliases(),
      xref: [
        exclude: [
          CSV,
          CSV.Encode,
          JSON,
          :mnesia,
          :msacc,
          :observer_cli,
          :public_key,
          :pubkey_cert,
          :rabbit,
          :rabbit_control_misc,
          :rabbit_data_coercion,
          :rabbit_env,
          :rabbit_event,
          :rabbit_file,
          :rabbit_net,
          :rabbit_log,
          :rabbit_misc,
          :rabbit_mnesia,
          :rabbit_mnesia_rename,
          :rabbit_nodes_common,
          :rabbit_pbe,
          :rabbit_plugins,
          :rabbit_resource_monitor_misc,
          :stdout_formatter
        ]
      ]
   ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger],
     env: [scopes: ['rabbitmq-plugins': :plugins,
                    rabbitmqctl: :ctl,
                    'rabbitmq-diagnostics': :diagnostics,
                    'rabbitmq-queues': :queues,
                    'rabbitmq-streams': :streams,
                    'rabbitmq-upgrade': :upgrade]]
    ]
    |> add_modules(Mix.env)
  end


  defp add_modules(app, :test) do
    # There are issues with building a package without this line ¯\_(ツ)_/¯
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
                    RabbitMQ.CLI.Seagull.Commands.SeagullCommand,
                    RabbitMQ.CLI.Seagull.Commands.PacificGullCommand,
                    RabbitMQ.CLI.Seagull.Commands.HerringGullCommand,
                    RabbitMQ.CLI.Seagull.Commands.HermannGullCommand,
                    RabbitMQ.CLI.Wolf.Commands.CanisLupusCommand,
                    RabbitMQ.CLI.Wolf.Commands.CanisLatransCommand,
                    RabbitMQ.CLI.Wolf.Commands.CanisAureusCommand
                  ]
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
  #
  # CAUTION: Dependencies which are shipped with RabbitMQ *MUST* com
  # from Hex.pm! Therefore it's ok to fetch dependencies from Git if
  # they are test dependencies or it is temporary while testing a patch.
  # But that's about it. If in doubt, use Hex.pm!
  #
  # The reason is that we have some Makefile code to put dependencies
  # from Hex.pm in RabbitMQ source archive (the source archive must be
  # self-contained and RabbitMQ must be buildable offline). However, we
  # don't have the equivalent for other methods.
  defp deps() do
    elixir_deps = [
      {:json, "~> 1.4.1"},
      {:csv, "~> 2.4.0"},
      {:stdout_formatter, "~> 0.2.3"},
      {:observer_cli, "~> 1.6.1"},

      {:amqp, "~> 2.1.0", only: :test},
      {:dialyxir, "~> 0.5", only: :test, runtime: false},
      {:temp, "~> 0.4", only: :test},
      {:x509, "~> 0.7", only: :test}
    ]

    rabbitmq_deps = case System.get_env("DEPS_DIR") do
      nil ->
        # rabbitmq_cli is built as a standalone Elixir application.
        [
          {:rabbit_common, "~> 3.8.0"},
          {:amqp_client, "~> 3.8.0", only: :test}
        ]
      deps_dir ->
        # rabbitmq_cli is built as part of RabbitMQ.

        # Mix is confused by any `rebar.{config,lock}` we might have left in
        # `rabbit_common` or `amqp_client`. So just remove those files to be
        # safe, as they are generated when we publish to Hex.pm only.
        for dir <- ["rabbit_common", "amqp_client"] do
          for file <- ["rebar.config", "rebar.lock"] do
            File.rm(Path.join([deps_dir, dir, file]))
          end
        end

        # We disable compilation for rabbit_common and amqp_client
        # because Erlang.mk already built them.
        [
          {
            :rabbit_common,
            path: Path.join(deps_dir, "rabbit_common"),
            compile: false,
            override: true
          },
          {
            :amqp_client,
            path: Path.join(deps_dir, "amqp_client"),
            compile: false,
            override: true,
            only: :test
          },
        ]
    end

    elixir_deps ++ rabbitmq_deps
  end

  defp aliases do
    [
      make_deps: [
        "deps.get",
        "deps.compile",
      ],
      make_app: [
        "compile",
        "escript.build",
      ],
      make_all: [
        "deps.get",
        "deps.compile",
        "compile",
        "escript.build",
      ],
      make_all_in_src_archive: [
        "deps.get --only prod",
        "deps.compile",
        "compile",
        "escript.build",
      ],
    ]
  end
end
