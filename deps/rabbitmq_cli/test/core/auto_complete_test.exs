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


defmodule AutoCompleteTest do
  use ExUnit.Case, async: false

  @subject Rabbitmq.CLI.AutoComplete


  test "Auto-completes a command" do
    ["canis_aureus", "canis_latrans", "canis_lupus"] = @subject.complete("rabbitmqctl", ["canis"])
    ["canis_aureus", "canis_latrans", "canis_lupus"] = @subject.complete("rabbitmqctl", ["canis_"])
    ["canis_latrans", "canis_lupus"] = @subject.complete("rabbitmqctl", ["canis_l"])
    ["canis_latrans"] = @subject.complete("rabbitmqctl", ["canis_la"])
    ["canis_aureus"] = @subject.complete("rabbitmqctl", ["canis_a"])
    ["canis_aureus"] = @subject.complete("rabbitmqctl", ["--node", "foo", "--quet", "canis_a"])
  end

  test "Auto-completes default options if command is not specified" do
    ["--vhost"] = @subject.complete("rabbitmqctl", ["--vh"])
    ## Prints script_name as script-name
    ["--script-name"] = @subject.complete("rabbitmqctl", ["--script"])
    ["--script-name"] = @subject.complete("rabbitmqctl", ["--node", "foo", "--script"])
  end

  test "Auto-completes the command options if full command is specified" do
    ["--colour", "--dingo", "--dog"] = @subject.complete("rabbitmqctl", ["canis_lupus", "-"])
    ["--colour", "--dingo", "--dog"] = @subject.complete("rabbitmqctl", ["canis_lupus", "--"])
    ["--dingo", "--dog"] = @subject.complete("rabbitmqctl", ["canis_lupus", "--d"])
  end

  test "Auto-completes scoped command" do
    ["enable"] = @subject.complete("rabbitmq-plugins", ["enab"])
    scopes = Application.get_env(:rabbitmqctl, :scopes)
    scopes_with_wolf = Keyword.put(scopes, :rabbitmq_wolf, :wolf)
    Application.put_env(:rabbitmqctl, :scopes, scopes_with_wolf)
    on_exit(fn() ->
      Application.put_env(:rabbitmqctl, :scopes, scopes)
    end)

    ["canis_aureus", "canis_latrans", "canis_lupus"] = @subject.complete("rabbitmq_wolf", ["canis"])
  end

  test "Auto-completes scoped command with --script-name flag" do
    ["enable"] = @subject.complete("rabbitmqctl", ["--script-name", "rabbitmq-plugins", "enab"])
  end
end

defmodule RabbitMQ.CLI.Wolf.Commands.CanisLupusCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["canis_lupus"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def switches(), do: [colour: :string, dingo: :boolean, dog: :boolean]
  def scopes, do: [:ctl, :wolf]
end

defmodule RabbitMQ.CLI.Wolf.Commands.CanisLatransCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["canis_latrans"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def scopes, do: [:ctl, :wolf]
end

defmodule RabbitMQ.CLI.Wolf.Commands.CanisAureusCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["canis_aureus"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
  def scopes, do: [:ctl, :wolf]
end
