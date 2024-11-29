%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_control_helper).

-export([command/2, command/3, command/4, command_with_output/4, format_command/4]).
-export([async_command/4, wait_for_async_command/1]).

command(Command, Node, Args) ->
    command(Command, Node, Args, []).

command(Command, Node) ->
    command(Command, Node, [], []).

command(Command, Node, Args, Opts) ->
    case command_with_output(Command, Node, Args, Opts) of
        {ok, _} -> ok;
        ok      -> ok;
        Error   -> Error
    end.

async_command(Command, Node, Args, Opts) ->
    Self = self(),
    spawn(fun() ->
                  Reply = (catch command(Command, Node, Args, Opts)),
                  Self ! {async_command, Node, Reply}
          end).

wait_for_async_command(Node) ->
    receive
        {async_command, N, Reply} when N == Node ->
            Reply
    after 600000 ->
            timeout
    end.

command_with_output(Command, Node, Args, Opts) ->
    Formatted = format_command(Command, Node, Args, Opts),
    Mod = 'Elixir.RabbitMQCtl', %% To silence a Dialyzer warning.
    CommandResult = Mod:exec_command(
        Formatted, fun(Output,_,_) -> Output end),
    ct:pal("Executed command ~tp against node ~tp~nResult: ~tp~n", [Formatted, Node, CommandResult]),
    CommandResult.

format_command(Command, Node, Args, Opts) ->
    Formatted = io_lib:format("~tp ~ts ~ts",
                              [Command,
                               format_args(Args),
                               format_options([{"--node", Node} | Opts])]),
    Mod = 'Elixir.OptionParser', %% To silence a Dialyzer warning.
    Mod:split(iolist_to_binary(Formatted)).

format_args(Args) ->
    iolist_to_binary([ io_lib:format("~tp ", [Arg]) || Arg <- Args ]).

format_options(Opts) ->
    EffectiveOpts = [{"--script-name", "rabbitmqctl"} | Opts],
    iolist_to_binary([io_lib:format("~ts=~tp ", [Key, Value])
                      || {Key, Value} <- EffectiveOpts ]).

