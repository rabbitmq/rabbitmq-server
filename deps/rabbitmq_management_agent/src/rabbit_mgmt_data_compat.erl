%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_data_compat).

-export([fill_get_empty_queue_metric/1,
         fill_consumer_active_fields/1,
         fill_drop_unroutable_metric/1]).

fill_get_empty_queue_metric(Slide) ->
    exometer_slide:map(
      fun
          (Value) when is_tuple(Value) andalso size(Value) =:= 8 ->
              Value;
          (Value) when is_tuple(Value) andalso size(Value) =:= 7 ->
              %% Inject a 0 for the new metric
              list_to_tuple(
                tuple_to_list(Value) ++ [0]);
          (Value) ->
              Value
      end, Slide).

fill_drop_unroutable_metric(Slide) ->
    exometer_slide:map(
      fun
          (Value) when is_tuple(Value) andalso size(Value) =:= 4 ->
              Value;
          (Value) when is_tuple(Value) andalso size(Value) =:= 3 ->
              %% Inject a 0
              list_to_tuple(
                tuple_to_list(Value) ++ [0]);
          (Value) ->
              Value
      end, Slide).

fill_consumer_active_fields(ConsumersStatsList) ->
    [case proplists:get_value(active, ConsumerStats) of
         undefined ->
             [{active, true},
              {activity_status, up}
              | ConsumerStats];
         _ ->
             ConsumerStats
     end
     || ConsumerStats <- ConsumersStatsList].
