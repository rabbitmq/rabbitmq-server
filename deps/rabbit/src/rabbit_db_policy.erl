%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_policy).

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

update(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    rabbit_db:run(
      #{mnesia => fun() -> update_in_mnesia(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) end,
        khepri => fun() -> update_in_khepri(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) end
       }).

%% [1] We need to prevent this from becoming O(n^2) in a similar
%% manner to rabbit_binding:remove_for_{source,destination}. So see
%% the comment in rabbit_binding:lock_route_tables/0 for more rationale.
update_in_mnesia(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    Tabs = [rabbit_queue,    rabbit_durable_queue,
            rabbit_exchange, rabbit_durable_exchange],
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              _ = [mnesia:lock({table, T}, write) || T <- Tabs], %% [1]
              Exchanges0 = rabbit_db_exchange:get_all(VHost),
              Queues0 = rabbit_db_queue:get_all(VHost),
              Exchanges = [GetUpdatedExchangeFun(X) || X <- Exchanges0],
              Queues = [GetUpdatedQueueFun(Q) || Q <- Queues0],
              {[update_exchange_policies(Map, fun rabbit_db_exchange:update_in_mnesia_tx/2)
                || Map <- Exchanges, is_map(Map)],
               [update_queue_policies(Map, fun rabbit_db_queue:update_in_mnesia_tx/2)
                || Map <- Queues, is_map(Map)]}
      end).

update_in_khepri(VHost, GetUpdatedExchangeFun, GetUpdatedQueueFun) ->
    Exchanges0 = rabbit_db_exchange:get_all(VHost),
    Queues0 = rabbit_db_queue:get_all(VHost),
    Exchanges = [GetUpdatedExchangeFun(X) || X <- Exchanges0],
    Queues = [GetUpdatedQueueFun(Q) || Q <- Queues0],
    rabbit_khepri:transaction(
      fun() ->
              {[update_exchange_policies(Map, fun rabbit_db_exchange:update_in_khepri_tx/2)
                || Map <- Exchanges, is_map(Map)],
               [update_queue_policies(Map, fun rabbit_db_queue:update_in_khepri_tx/2)
                || Map <- Queues, is_map(Map)]}
      end, rw).

update_exchange_policies(#{exchange := X = #exchange{name = XName},
                           update_function := UpdateFun}, StoreFun) ->
    NewExchange = StoreFun(XName, UpdateFun),
    case NewExchange of
        #exchange{} = X1 -> {X, X1};
        not_found        -> {X, X }
    end.

update_queue_policies(#{queue := Q0, update_function := UpdateFun}, StoreFun) ->
    QName = amqqueue:get_name(Q0),
    NewQueue = StoreFun(QName, UpdateFun),
    case NewQueue of
        Q1 when ?is_amqqueue(Q1) ->
            {Q0, Q1};
        not_found ->
            {Q0, Q0}
    end.
