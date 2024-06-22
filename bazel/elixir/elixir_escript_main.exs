defmodule ElixirEscriptMain do
  # https://github.com/elixir-lang/elixir/blob/99785cc16be096d02012ad889ca51b5045b599a4/lib/mix/lib/mix/tasks/escript.build.ex#L327
  def gen_main(project, name, module, app, language) do
    config_path = project[:config_path]

    compile_config =
      if File.regular?(config_path) do
        config = Config.Reader.read!(config_path, env: Mix.env(), target: Mix.target())
        Macro.escape(config)
      else
        []
      end

    runtime_path = config_path |> Path.dirname() |> Path.join("runtime.exs")

    runtime_config =
      if File.regular?(runtime_path) do
        File.read!(runtime_path)
      end

    module_body =
      quote do
        @spec main(OptionParser.argv()) :: any
        def main(args) do
          unquote(main_body_for(language, module, app, compile_config, runtime_config))
        end

        defp load_config(config) do
          each_fun = fn {app, kw} ->
            set_env_fun = fn {k, v} -> :application.set_env(app, k, v, persistent: true) end
            :lists.foreach(set_env_fun, kw)
          end

          :lists.foreach(each_fun, config)
          :ok
        end

        defp start_app(nil) do
          :ok
        end

        defp start_app(app) do
          case :application.ensure_all_started(app) do
            {:ok, _} ->
              :ok

            {:error, {app, reason}} ->
              formatted_error =
                case :code.ensure_loaded(Application) do
                  {:module, Application} -> Application.format_error(reason)
                  {:error, _} -> :io_lib.format(~c"~p", [reason])
                end

              error_message = [
                "ERROR! Could not start application ",
                :erlang.atom_to_binary(app, :utf8),
                ": ",
                formatted_error,
                ?\n
              ]

              io_error(error_message)
              :erlang.halt(1)
          end
        end

        defp io_error(message) do
          :io.put_chars(:standard_error, message)
        end
      end

    {:module, ^name, binary, _} = Module.create(name, module_body, Macro.Env.location(__ENV__))
    [{~c"#{name}.beam", binary}]
  end

  defp main_body_for(:elixir, module, app, compile_config, runtime_config) do
    config =
      if runtime_config do
        quote do
          runtime_config =
            Config.Reader.eval!(
              "config/runtime.exs",
              unquote(runtime_config),
              env: unquote(Mix.env()),
              target: unquote(Mix.target()),
              imports: :disabled
            )

          Config.Reader.merge(unquote(compile_config), runtime_config)
        end
      else
        compile_config
      end

    quote do
      case :application.ensure_all_started(:elixir) do
        {:ok, _} ->
          args = Enum.map(args, &List.to_string(&1))
          System.argv(args)
          load_config(unquote(config))
          start_app(unquote(app))
          Kernel.CLI.run(fn _ -> unquote(module).main(args) end)

        error ->
          io_error(["ERROR! Failed to start Elixir.\n", :io_lib.format(~c"error: ~p~n", [error])])
          :erlang.halt(1)
      end
    end
  end
end

output = System.get_env("OUT")
IO.puts("Will write to " <> output)

project = [
  config_path: System.get_env("CONFIG_PATH", "config/config.exs"),
]
app = String.to_atom(System.get_env("APP"))
name = String.to_atom(Atom.to_string(app) <> "_escript")
module = String.to_atom(System.get_env("MAIN_MODULE"))

:application.ensure_all_started(:mix)
Mix.State.start_link(:none)
[{_, bytecode}] = ElixirEscriptMain.gen_main(project, name, module, app, :elixir)

{:ok, file} = File.open(output, [:write])
IO.binwrite(file, bytecode)
File.close(file)

IO.puts("done.")
