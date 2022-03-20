%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ShovelStatusCommand').

-include("rabbit_shovel.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref({'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}).

-export([
         usage/0,
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
    <<"shovel_status">>.

usage_doc_guides() ->
    [?SHOVEL_GUIDE_URL].

description() ->
    <<"Displays status of Shovel on a node">>.

help_section() ->
    {plugin, shovel}.

flags() ->
    [].

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.Table'.

validate(_,_) ->
    ok.

merge_defaults(A,O) ->
    {A, O}.

banner(_, #{node := Node}) ->
    erlang:iolist_to_binary([<<"Shovel status on node ">>,
                             atom_to_binary(Node, utf8)]).

run(_Args, #{node := Node}) ->
    case rabbit_misc:rpc_call(Node, rabbit_shovel_status, status, []) of
        {badrpc, _} = Error ->
            Error;
        Status ->
            {stream, Status}
    end.

switches() ->
    [].

aliases() ->
    [].

output({stream, ShovelStatus}, _Opts) ->
    Formatted = [fmt_name(Name,
                          fmt_status(Status,
                                     #{type => Type,
                                       last_changed => fmt_ts(Timestamp)}))
                 || {Name, Type, Status, Timestamp} <- ShovelStatus],
    {stream, Formatted};
output(E, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(E).

scopes() ->
    ['ctl', 'diagnostics'].

%%----------------------------------------------------------------------------
%% Formatting
%%----------------------------------------------------------------------------
fmt_name({Vhost, Name}, Map) ->
    Map#{name => Name, vhost => Vhost};
fmt_name(Name, Map) ->
    %% Static shovel names don't contain the vhost
    Map#{name => Name}.

fmt_ts({{YY, MM, DD}, {Hour, Min, Sec}}) ->
    erlang:list_to_binary(
      io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
                    [YY, MM, DD, Hour, Min, Sec])).

fmt_status({'running' = St, Proplist}, Map) ->
    maps:merge(Map#{state => St,
                    source_protocol => proplists:get_value(src_protocol, Proplist,
                                                           undefined),
                    source => proplists:get_value(src_uri, Proplist),
                    destination_protocol => proplists:get_value(dest_protocol, Proplist, undefined),
                    destination => proplists:get_value(dest_uri, Proplist),
                    termination_reason => <<>>}, details_to_map(Proplist));
fmt_status('starting' = St, Map) ->
    Map#{state => St,
         source => <<>>,
         destination => <<>>,
         termination_reason => <<>>};
fmt_status({'terminated' = St, Reason}, Map) ->
    Map#{state => St,
         termination_reason => list_to_binary(io_lib:format("~p", [Reason])),
         source => <<>>,
         destination => <<>>}.

details_to_map(Proplist) ->
    Keys = [{src_address, source_address}, {src_queue, source_queue},
            {src_exchange, source_exchange}, {src_exchange_key, source_exchange_key},
            {dest_address, destination_address}, {dest_queue, destination_queue},
            {dest_exchange, destination_exchange}, {dest_exchange_key, destination_exchange_key}],
    maps:from_list([{New, proplists:get_value(Old, Proplist)}
                    || {Old, New} <- Keys, proplists:is_defined(Old, Proplist)]).
