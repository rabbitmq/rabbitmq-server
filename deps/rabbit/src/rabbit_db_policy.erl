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
%% transaction below. assert_unchanged/2 re-checks each record's
%% payload_version against that snapshot from inside the transaction,
%% aborting it -- and triggering a retry from scratch -- if a concurrent
%% policy update changed the record in the meantime.
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
    %% khepri_tx:abort/1, which is how assert_unchanged/2 signals a
    %% payload_version mismatch.
    try
        rabbit_khepri:transaction(
          fun() ->
                  {[update_exchange_policies(Map, ExchangeVsns)
                    || Map <- Exchanges, is_map(Map)],
                   [update_queue_policies(Map, QueueVsns)
                    || Map <- Queues, is_map(Map)]}
          end, rw)
    catch
        throw:{error, ?khepri_error(mismatching_node, _)} ->
            update(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun)
    end.

update_exchange_policies(#{exchange := X = #exchange{name = XName},
                           update_function := UpdateFun}, Vsns) ->
    Vsn = maps:get(XName, Vsns),
    assert_unchanged(rabbit_db_exchange:khepri_exchange_path(XName), Vsn),
    NewExchange = rabbit_db_exchange:update_in_khepri_tx(XName, UpdateFun),
    case NewExchange of
        #exchange{} = X1 -> {X, X1};
        not_found        -> {X, X }
    end.

update_queue_policies(#{queue := Q0, update_function := UpdateFun}, Vsns) ->
    QName = amqqueue:get_name(Q0),
    Vsn = maps:get(QName, Vsns),
    assert_unchanged(rabbit_db_queue:khepri_queue_path(QName), Vsn),
    NewQueue = rabbit_db_queue:update_in_khepri_tx(QName, UpdateFun),
    case NewQueue of
        Q1 when ?is_amqqueue(Q1) ->
            {Q0, Q1};
        not_found ->
            {Q0, Q0}
    end.

%% Aborts the enclosing transaction if the record at `Path' has changed
%% since `Vsn' was read outside of it. A record that was deleted in the
%% meantime is left alone here: the subsequent update_in_khepri_tx/2
%% call already treats that as `not_found'.
%%
%% khepri_tx_adv:get/1 runs wherever this transaction gets applied,
%% which during a rolling upgrade can be a cluster member still on an
%% older Khepri that doesn't wrap the result by `Path' (see e.g.
%% rabbit_db_queue:do_delete_transient_queues_in_khepri_tx/2 for the
%% same pattern), so both shapes have to be handled.
assert_unchanged(Path, Vsn) ->
    UsesUniformWriteRet = try
                              khepri_tx:does_api_comply_with(uniform_write_ret)
                          catch
                              error:undef -> false
                          end,
    case khepri_tx_adv:get(Path) of
        {ok, #{Path := #{payload_version := Vsn}}} when UsesUniformWriteRet ->
            ok;
        {ok, #{Path := #{}}} when UsesUniformWriteRet ->
            khepri_tx:abort(?khepri_error(mismatching_node, #{node_path => Path}));
        {ok, #{payload_version := Vsn}} when not UsesUniformWriteRet ->
            ok;
        {ok, #{}} when not UsesUniformWriteRet ->
            khepri_tx:abort(?khepri_error(mismatching_node, #{node_path => Path}));
        {error, ?khepri_error(node_not_found, _)} ->
            ok;
        {error, Reason} ->
            khepri_tx:abort(Reason)
    end.
