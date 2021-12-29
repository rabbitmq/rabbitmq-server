%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.FederationStatusCommand').

-include("rabbit_federation.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref({'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}).

-export([
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         flags/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         switches/0,
         aliases/0,
         output/2,
         scopes/0,
         formatter/0,
         help_section/0,
         description/0
        ]).


%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------
usage() ->
    <<"federation_status [--only-down]">>.

usage_additional() ->
    [
      {<<"--only-down">>, <<"only display links that failed or are not currently connected">>}
    ].

usage_doc_guides() ->
    [?FEDERATION_GUIDE_URL].

help_section() ->
    {plugin, federation}.

description() ->
    <<"Displays federation link status">>.

flags() ->
    [].

validate(_,_) ->
    ok.

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.Erlang'.

merge_defaults(A, Opts) ->
    {A, maps:merge(#{only_down => false}, Opts)}.

banner(_, #{node := Node, only_down := true}) ->
    erlang:iolist_to_binary([<<"Listing federation links which are down on node ">>,
                             atom_to_binary(Node, utf8), <<"...">>]);
banner(_, #{node := Node, only_down := false}) ->
    erlang:iolist_to_binary([<<"Listing federation links on node ">>,
                             atom_to_binary(Node, utf8), <<"...">>]).

run(_Args, #{node := Node, only_down := OnlyDown}) ->
    case rabbit_misc:rpc_call(Node, rabbit_federation_status, status, []) of
        {badrpc, _} = Error ->
            Error;
        Status ->
            {stream, filter(Status, OnlyDown)}
    end.

switches() ->
    [{only_down, boolean}].

aliases() ->
    [].

output({stream, FederationStatus}, _) ->
    Formatted = [begin
                     Timestamp = proplists:get_value(timestamp, St),
                     Map0 = maps:remove(timestamp, maps:from_list(St)),
                     Map1 = maps:merge(#{queue => <<>>,
                                         exchange => <<>>,
                                         upstream_queue => <<>>,
                                         upstream_exchange => <<>>,
                                         local_connection => <<>>,
                                         error => <<>>}, Map0),
                     Map1#{last_changed => fmt_ts(Timestamp)}
                 end || St <- FederationStatus],
    {stream, Formatted};
output(E, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(E).

scopes() ->
    ['ctl', 'diagnostics'].

%%----------------------------------------------------------------------------
%% Formatting
%%----------------------------------------------------------------------------
fmt_ts({{YY, MM, DD}, {Hour, Min, Sec}}) ->
    erlang:list_to_binary(
      io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
                    [YY, MM, DD, Hour, Min, Sec])).

filter(Status, _OnlyDown = false) ->
    Status;
filter(Status, _OnlyDown = true) ->
    [St || St <- Status,
           not lists:member(proplists:get_value(status, St), [running, starting])].
