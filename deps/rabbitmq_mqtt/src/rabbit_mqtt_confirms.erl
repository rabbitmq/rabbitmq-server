%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_confirms).

-include("rabbit_mqtt_packet.hrl").
-compile({no_auto_import, [size/1]}).

-export([init/0,
         insert/3,
         confirm/3,
         reject/2,
         remove_queue/2,
         size/1,
         contains/2]).

-type queue_name() :: rabbit_amqqueue:name().
-opaque state() :: #{packet_id() => #{queue_name() => ok}}.
-export_type([state/0]).

-spec init() -> state().
init() ->
    maps:new().

-spec size(state()) -> non_neg_integer().
size(State) ->
    maps:size(State).

-spec contains(packet_id(), state()) -> boolean().
contains(PktId, State) ->
    maps:is_key(PktId, State).

-spec insert(packet_id(), [queue_name()], state()) -> state().
insert(PktId, QNames, State)
  when is_integer(PktId) andalso
       PktId > 0 andalso
       not is_map_key(PktId, State) ->
    QMap = maps:from_keys(QNames, ok),
    maps:put(PktId, QMap, State).

-spec confirm([packet_id()], queue_name(), state()) ->
    {[packet_id()], state()}.
confirm(PktIds, QName, State0) ->
    lists:foldl(fun(PktId, Acc) ->
                        confirm_one(PktId, QName, Acc)
                end, {[], State0}, PktIds).

-spec reject([packet_id()], state()) ->
    {[packet_id()], state()}.
reject(PktIds, State0) ->
    lists:foldl(fun(PktId, Acc = {Rejs, S0}) ->
                        case maps:take(PktId, S0) of
                            {_, S} ->
                                {[PktId | Rejs], S};
                            error ->
                                Acc
                        end
                end, {[], State0}, PktIds).

%% idempotent
-spec remove_queue(queue_name(), state()) ->
    {[packet_id()], state()}.
remove_queue(QName, State) ->
    PktIds = maps:fold(
               fun(PktId, QMap, PktIds)
                     when is_map_key(QName, QMap) ->
                       [PktId | PktIds];
                  (_, _, PktIds) ->
                       PktIds
               end, [], State),
    confirm(PktIds, QName, State).

%% INTERNAL

confirm_one(PktId, QName, {PktIds, State0})
  when is_integer(PktId) ->
    case maps:take(PktId, State0) of
        {QMap0, State1}
          when is_map_key(QName, QMap0)
               andalso map_size(QMap0) =:= 1 ->
            %% last queue confirm
            {[PktId| PktIds], State1};
        {QMap0, State1} ->
            QMap = maps:remove(QName, QMap0),
            State = maps:put(PktId, QMap, State1),
            {PktIds, State};
        error ->
            {PktIds, State0}
    end.
