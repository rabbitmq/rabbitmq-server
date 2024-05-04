%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_dead_letter).
-include("mc.hrl").

-export([publish/5,
         detect_cycles/3]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-type reason() :: expired | rejected | maxlen | delivery_limit.
-export_type([reason/0]).

%%----------------------------------------------------------------------------

-spec publish(mc:state(), reason(), rabbit_types:exchange(),
              undefined | rabbit_types:routing_key(), rabbit_amqqueue:name()) ->
    ok.
publish(Msg0, Reason, #exchange{name = XName} = DLX, RK,
        #resource{name = SourceQName}) ->
    DLRKeys = case RK of
                  undefined ->
                      mc:routing_keys(Msg0);
                  _ ->
                      [RK]
              end,
    Env = case rabbit_feature_flags:is_enabled(?FF_MC_DEATHS_V2) of
              true -> #{};
              false -> #{?FF_MC_DEATHS_V2 => false}
          end,
    Msg1 = mc:record_death(Reason, SourceQName, Msg0, Env),
    {Ttl, Msg2} = mc:take_annotation(dead_letter_ttl, Msg1),
    Msg3 = mc:set_ttl(Ttl, Msg2),
    Msg4 = mc:set_annotation(?ANN_ROUTING_KEYS, DLRKeys, Msg3),
    DLMsg = mc:set_annotation(?ANN_EXCHANGE, XName#resource.name, Msg4),
    Routed0 = rabbit_exchange:route(DLX, DLMsg, #{return_binding_keys => true}),
    {Cycles, Routed} = detect_cycles(Reason, DLMsg, Routed0),
    lists:foreach(fun log_cycle_once/1, Cycles),
    Qs0 = rabbit_amqqueue:lookup_many(Routed),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    _ = rabbit_queue_type:deliver(Qs, DLMsg, #{}, stateless),
    ok.

detect_cycles(rejected, _Msg, Queues) ->
    %% shortcut
    {[], Queues};
detect_cycles(_Reason, Msg, Queues) ->
    {Cycling,
     NotCycling} = lists:partition(fun(#resource{name = Queue}) ->
                                           mc:is_death_cycle(Queue, Msg);
                                      ({#resource{name = Queue}, _RouteInfos}) ->
                                           mc:is_death_cycle(Queue, Msg)
                                   end, Queues),
    Names = mc:death_queue_names(Msg),
    Cycles = lists:map(fun(#resource{name = Q}) ->
                               [Q | Names];
                          ({#resource{name = Q}, _RouteInfos}) ->
                               [Q | Names]
                       end, Cycling),
    {Cycles, NotCycling}.

log_cycle_once(Cycle) ->
    %% Using a hash won't eliminate this as a potential memory leak but it will
    %% reduce the potential amount of memory used whilst probably being "good enough".
    Key = {dead_letter_cycle, erlang:phash2(Cycle)},
    case get(Key) of
        true ->
            ok;
        undefined ->
            rabbit_log:warning(
              "Message dropped because the following list of queues (ordered by "
              "death recency) contains a dead letter cycle without reason 'rejected'. "
              "This list will not be logged again: ~tp",
              [Cycle]),
            put(Key, true)
    end.
