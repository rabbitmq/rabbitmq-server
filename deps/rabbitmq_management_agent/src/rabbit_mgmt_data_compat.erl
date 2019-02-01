%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_data_compat).

-export([fill_get_empty_queue_metric/1,
         drop_get_empty_queue_metric/1]).

fill_get_empty_queue_metric(Slide) ->
    exometer_slide:map(
      fun
          (Value) when is_tuple(Value) andalso size(Value) =:= 8 ->
              Value;
          (Value) when is_tuple(Value) andalso size(Value) =:= 7 ->
              %% We want to remote the last element, which is
              %% the count of basic.get on empty queues.
              list_to_tuple(
                tuple_to_list(Value) ++ [0]);
          (Value) ->
              Value
      end, Slide).

drop_get_empty_queue_metric(Slide) ->
    exometer_slide:map(
      fun
          (Value) when is_tuple(Value) andalso size(Value) =:= 8 ->
              %% We want to remote the last element, which is
              %% the count of basic.get on empty queues.
              list_to_tuple(
                lists:sublist(
                  tuple_to_list(Value), size(Value) - 1));
          (Value) when is_tuple(Value) andalso size(Value) =:= 7 ->
              Value;
          (Value) ->
              Value
      end, Slide).
