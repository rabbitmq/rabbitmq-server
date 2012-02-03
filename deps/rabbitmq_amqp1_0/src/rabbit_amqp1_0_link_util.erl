%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_link_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-export([check_queue/2, check_exchange/2, create_queue/2, create_bound_queue/3,
         parse_destination/2, parse_destination/1, queue_address/1, outcomes/1,
         protocol_error/3, ctag_to_handle/1, handle_to_ctag/1]).

-define(EXCHANGE_SUB_LIFETIME, "delete-on-close").
-define(DEFAULT_OUTCOME, #'v1_0.released'{}).
-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED]).

%% Check that a queue exists
check_queue(QueueName, DCh) when is_list(QueueName) ->
    check_queue(list_to_binary(QueueName), DCh);
check_queue(QueueName, DCh) ->
    QDecl = #'queue.declare'{queue = QueueName, passive = true},
    case catch amqp_channel:call(DCh, QDecl) of
        {'EXIT', _Reason}     -> {error, not_found};
        #'queue.declare_ok'{} -> {ok, QueueName}
    end.

check_exchange(ExchangeName, DCh) when is_list(ExchangeName) ->
    check_exchange(list_to_binary(ExchangeName), DCh);
check_exchange(ExchangeName, DCh) when is_binary(ExchangeName) ->
    XDecl = #'exchange.declare'{ exchange = ExchangeName, passive = true },
    case catch amqp_channel:call(DCh, XDecl) of
        {'EXIT', _Reason}        -> {error, not_found};
        #'exchange.declare_ok'{} -> {ok, ExchangeName}
    end.

%% TODO Lifetimes: we approximate these with auto_delete.
create_queue(_Lifetime, DCh) ->
    #'queue.declare_ok'{queue = QueueName} =
        amqp_channel:call(DCh, #'queue.declare'{auto_delete = true}),
    {ok, QueueName}.

create_bound_queue(ExchangeName, RoutingKey, DCh) ->
    {ok, QueueName} = create_queue(?EXCHANGE_SUB_LIFETIME, DCh),
    %% Don't both ensuring the channel, the previous should have done it
    #'queue.bind_ok'{} =
        amqp_channel:call(DCh, #'queue.bind'{exchange = ExchangeName,
                                             queue = QueueName,
                                             routing_key = RoutingKey}),
    {ok, QueueName}.

parse_destination(Destination, Enc) when is_binary(Destination) ->
    parse_destination(unicode:characters_to_list(Destination, Enc)).

parse_destination(Destination) when is_list(Destination) ->
    case re:split(Destination, "/", [{return, list}]) of
        [Name] ->
            ["queue", Name];
        ["", Type | Tail] when
              Type =:= "queue" orelse Type =:= "exchange" ->
            [Type | Tail];
        _Else ->
            {error, {malformed_address, Destination}}
    end.

queue_address(QueueName) when is_binary(QueueName) ->
    <<"/queue/", QueueName/binary>>.

outcomes(Source) ->
    {DefaultOutcome, Outcomes} =
        case Source of
            #'v1_0.source' {
                      default_outcome = DO,
                      outcomes = Os
                     } ->
                DO1 = case DO of
                          undefined -> ?DEFAULT_OUTCOME;
                          _         -> DO
                      end,
                Os1 = case Os of
                          undefined -> ?OUTCOMES;
                          _         -> Os
                      end,
                {DO1, Os1};
            _ ->
                {?DEFAULT_OUTCOME, ?OUTCOMES}
        end,
    case [O || O <- Outcomes, not lists:member(O, ?OUTCOMES)] of
        []   -> {DefaultOutcome, Outcomes};
        Bad  -> protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                               "Outcomes not supported: ~p", [Bad])
    end.

protocol_error(Condition, Msg, Args) ->
    exit(#'v1_0.error'{
        condition = Condition,
        description = {utf8, list_to_binary(
                               lists:flatten(io_lib:format(Msg, Args)))}
       }).

handle_to_ctag({uint, H}) ->
    <<"ctag-", H:32/integer>>.

ctag_to_handle(<<"ctag-", H:32/integer>>) ->
    {uint, H}.
