%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
