%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Diagnostics.Commands.ConsistentHashExchangeRingStateCommand').

-include_lib("rabbit_common/include/resource.hrl").
-include("rabbitmq_consistent_hash_exchange.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref([
    {'Elixir.RabbitMQ.CLI.Core.ExitCodes', exit_dataerr, 0},
    {'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}
]).

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

-import(rabbit_data_coercion, [to_binary/1]).

-define(NOT_FOUND_MESSAGE, <<"Exchange does not exist or is of a different type">>).

%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------

scopes() ->
    ['diagnostics'].

switches() ->
    [].

aliases() ->
    [].

flags() ->
    [].

merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{vhost => <<"/">>}, Opts)}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_Exchange], _Opts) ->
    ok;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

run([Exchange], #{node := Node, vhost := VirtualHost}) ->
    case rabbit_misc:rpc_call(Node, rabbit_exchange_type_consistent_hash, ring_state, [VirtualHost, Exchange]) of
        {badrpc, _} = Error ->
            Error;
        {badrpc, _, _} = Error ->
            Error;
        {error, _} = Error ->
            Error;
        {ok, State} ->
            {ok, State}
    end.

output({error, not_found}, #{node := Node, formatter := <<"json">>}) ->
    {error, #{
        <<"result">> => <<"error">>,
        <<"node">> => Node,
        <<"message">> => ?NOT_FOUND_MESSAGE
    }};
output({ok, #chx_hash_ring{exchange = Resource = #resource{name = Exchange}, bucket_map = Buckets}}, #{node := Node, formatter := <<"json">>}) ->
    {ok, #{
        <<"result">>   => <<"ok">>,
        <<"node">>     => Node,
        <<"exchange">> => Exchange,
        <<"message">>  => to_binary(rabbit_misc:format("Consistent hashing ring state for ~s",
                                                       [rabbit_misc:rs(Resource)])),
        <<"buckets">>  =>
            maps:from_list(lists:map(fun ({Key, #resource{kind = queue, name = Queue}}) ->
                                        {to_binary(Key), Queue}
                                     end, maps:to_list(Buckets)))
    }};
output({error, not_found}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_dataerr(), ?NOT_FOUND_MESSAGE};
output({ok, #chx_hash_ring{bucket_map = Buckets0}}, _Opts) ->
    Buckets = maps:map(fun(_Key, #resource{kind = queue, name = Queue}) -> Queue end, Buckets0),
    {ok, (ring_state_lines(Buckets))};
output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).

usage() ->
    <<"consistent_hash_exchange_ring_state <exchange>">>.

usage_additional() ->
    [].

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.String'.

usage_doc_guides() ->
    [].

help_section() ->
    {plugin, consistent_hash_exchange}.

description() ->
    <<"Displays consistent hashing exchange ring state">>.

banner([Exchange], #{vhost := VirtualHost}) ->
    erlang:iolist_to_binary([<<"Inspecting consistent hashing ring state for exchange ">>,
                             to_binary(Exchange),
                             <<" in virtual host ">>,
                             to_binary(rabbit_misc:format("'~s'", [VirtualHost])),
                             <<"...">>]).

%%
%% Implementation
%%

ring_state_lines(Buckets) ->
    Fun = fun (Key, QName, Acc) ->
            [to_binary(rabbit_misc:format("Ring index: ~b, queue: '~s'~n", [Key, QName])) | Acc]
          end,
    lists:usort(maps:fold(Fun, [], Buckets)).
