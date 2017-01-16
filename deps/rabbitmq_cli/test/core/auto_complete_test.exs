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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule AutoCompleteTest do
  use ExUnit.Case, async: false

  @subject Rabbitmq.CLI.AutoComplete


  test "Auto-completes a command" do
    ["canis_lupus", "canis_latrans", "canis_aureus"] = @subject.complete("canis")
    ["canis_lupus", "canis_latrans", "canis_aureus"] = @subject.complete("canis_")
    ["canis_lupus", "canis_latrans"] = @subject.complete("canis_l")
    ["canis_latrans"] = @subject.complete("canis_la")
    ["canis_aureus"] = @subject.complete("canis_a")
    ["canis_aureus"] = @subject.complete("--node foo --quet canis_a")
  end

  test "Auto-completes default options if command is not specified" do
    ["--vhost"] = @subject.complete("--vh")
    ## Prints script_name as script-name
    ["--script-name"] = @subject.complete("--script")
    ["--script-name"] = @subject.complete("--node foo --script")
  end

  test "Auto-completes the command options if full command is specified" do
    ["--colour", "--dingo", "--dog"] = @subject.complete("canis_lupus -")
    ["--colour", "--dingo", "--dog"] = @subject.complete("canis_lupus --")
    ["--dingo", "--dog"] = @subject.complete("canis_lupus --d")
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
end

defmodule RabbitMQ.CLI.Wolf.Commands.CanisLatransCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["canis_latrans"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end

defmodule RabbitMQ.CLI.Wolf.Commands.CanisAureusCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def usage(), do: ["canis_aureus"]
  def validate(_,_), do: :ok
  def merge_defaults(_,_), do: {[], %{}}
  def banner(_,_), do: ""
  def run(_,_), do: :ok
end
