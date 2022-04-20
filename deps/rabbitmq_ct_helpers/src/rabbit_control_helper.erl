%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_control_helper).

-export([command/2, command/3, command/4, command_with_output/4, format_command/4]).

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

command_with_output(Command, Node, Args, Opts) ->
    Formatted = format_command(Command, Node, Args, Opts),
    CommandResult = 'Elixir.RabbitMQCtl':exec_command(
        Formatted, fun(Output,_,_) -> Output end),
    ct:pal("Executed command ~p against node ~p~nResult: ~p~n", [Formatted, Node, CommandResult]),
    CommandResult.

format_command(Command, Node, Args, Opts) ->
    Formatted = io_lib:format("~tp ~ts ~ts",
                              [Command,
                               format_args(Args),
                               format_options([{"--node", Node} | Opts])]),
    'Elixir.OptionParser':split(iolist_to_binary(Formatted)).

format_args(Args) ->
    iolist_to_binary([ io_lib:format("~tp ", [Arg]) || Arg <- Args ]).

format_options(Opts) ->
    EffectiveOpts = [{"--script-name", "rabbitmqctl"} | Opts],
    iolist_to_binary([io_lib:format("~s=~tp ", [Key, Value])
                      || {Key, Value} <- EffectiveOpts ]).

