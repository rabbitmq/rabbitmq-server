%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_policy).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([update/3]).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(VHostName, UpdateXFun, UpdateQFun) -> Ret when
      VHostName :: vhost:name(),
      Exchange :: rabbit_types:exchange(),
      Queue :: amqqueue:amqqueue(),
      UpdateXFun :: fun((Exchange) -> #{exchange => Exchange,
                                        update_function => fun((Exchange) -> Exchange)}),
      UpdateQFun :: fun((Queue) -> #{queue => Queue,
                                     update_function => fun((Queue) -> Queue)}),
      Ret :: {[{Exchange, Exchange}], [{Queue, Queue}]}.

%% `UpdateXFun'/`UpdateQFun' decide the new policy to apply from a
%% snapshot of the vhost's exchanges/queues read here, outside of the
%% transaction below. If a *different* concurrent policy update commits
%% first, that snapshot is stale by the time this transaction runs: the
%% payload_version guard on each write (see rabbit_db_exchange/queue's
%% update_in_khepri_tx/3) detects that and aborts the whole transaction
%% -- discarding any puts already made during this attempt, since
%% khepri_tx:abort/1 unwinds via an exception rather than returning an
%% ordinary value the transaction would otherwise still commit -- and
%% retrying from scratch re-reads the now-current state instead of
%% blindly overwriting the concurrent update's effect.
update(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    case rabbit_khepri:adv_get_many(
           rabbit_db_exchange:khepri_exchange_path(VHost, #if_has_data{})) of
        {ok, ExchangeProps} ->
            case rabbit_khepri:adv_get_many(
                   rabbit_db_queue:khepri_queue_path(VHost, #if_has_data{})) of
                {ok, QueueProps} ->
                    update1(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun,
                            ExchangeProps, QueueProps);
                {error, _} = Error ->
                    error(Error)
            end;
        {error, _} = Error ->
            error(Error)
    end.

update1(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun,
        ExchangeProps, QueueProps) ->
    ExchangeVsns = maps:from_list(
                     [{XName, Vsn}
                      || #{data := #exchange{name = XName},
                           payload_version := Vsn} <- maps:values(ExchangeProps)]),
    QueueVsns = maps:from_list(
                  [{amqqueue:get_name(Q), Vsn}
                   || #{data := Q, payload_version := Vsn} <- maps:values(QueueProps),
                      ?is_amqqueue(Q)]),
    Exchanges = [GetUpdatedExchangeFun(X)
                 || #{data := X} <- maps:values(ExchangeProps)],
    Queues = [GetUpdatedQueueFun(Q)
              || #{data := Q} <- maps:values(QueueProps), ?is_amqqueue(Q)],
    %% rabbit_khepri:transaction/2 throws {error, Reason} (it doesn't
    %% return it as a value) whenever the transaction fun aborts via
    %% khepri_tx:abort/1, which is how update_in_khepri_tx/3 signals a
    %% payload_version mismatch.
    try
        rabbit_khepri:transaction(
          fun() ->
                  {[update_exchange_policies(
                      Map, ExchangeVsns,
                      fun rabbit_db_exchange:update_in_khepri_tx/3)
                    || Map <- Exchanges, is_map(Map)],
                   [update_queue_policies(
                      Map, QueueVsns,
                      fun rabbit_db_queue:update_in_khepri_tx/3)
                    || Map <- Queues, is_map(Map)]}
          end, rw)
    catch
        throw:{error, {khepri, mismatching_node, _}} ->
            update(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun)
    end.

update_exchange_policies(#{exchange := X = #exchange{name = XName},
                           update_function := UpdateFun}, Vsns, StoreFun) ->
    Vsn = maps:get(XName, Vsns),
    NewExchange = StoreFun(XName, Vsn, UpdateFun),
    case NewExchange of
        #exchange{} = X1 -> {X, X1};
        not_found        -> {X, X }
    end.

update_queue_policies(#{queue := Q0, update_function := UpdateFun}, Vsns, StoreFun) ->
    QName = amqqueue:get_name(Q0),
    Vsn = maps:get(QName, Vsns),
    NewQueue = StoreFun(QName, Vsn, UpdateFun),
    case NewQueue of
        Q1 when ?is_amqqueue(Q1) ->
            {Q0, Q1};
        not_found ->
            {Q0, Q0}
    end.
