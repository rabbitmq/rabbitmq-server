%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(mc).

-export([
         init/3,
         init/4,
         size/1,
         is/1,
         get_annotation/2,
         take_annotation/2,
         set_annotation/3,
         %% properties
         is_persistent/1,
         ttl/1,
         correlation_id/1,
         user_id/1,
         message_id/1,
         property/2,
         timestamp/1,
         priority/1,
         set_ttl/2,
         x_header/2,
         x_headers/1,
         routing_headers/2,
         exchange/1,
         routing_keys/1,
         %%
         convert/2,
         convert/3,
         protocol_state/1,
         prepare/2,
         record_death/4,
         is_death_cycle/2,
         death_queue_names/1
        ]).

-include("mc.hrl").

-type str() :: atom() | string() | binary().
-type internal_ann_key() :: atom().
-type x_ann_key() :: binary(). %% should begin with x- or ideally x-opt-
-type x_ann_value() :: str() | number() | TaggedValue :: tuple() | [x_ann_value()].
-type protocol() :: module().
-type annotations() :: #{internal_ann_key() => term(),
                         x_ann_key() => x_ann_value()}.
-type environment() :: #{atom() => term()}.
-type ann_key() :: internal_ann_key() | x_ann_key().
-type ann_value() :: term().

%% the protocol module must implement the mc behaviour
-record(?MODULE, {protocol :: protocol(),
                  %% protocol specific data term
                  data :: proto_state(),
                  %% any annotations done by the broker itself
                  %% such as recording the exchange / routing keys used
                  annotations = #{} :: annotations()
                 }).

-opaque state() :: #?MODULE{} | mc_compat:state().

-export_type([
              state/0,
              ann_key/0,
              ann_value/0,
              annotations/0
             ]).

-type proto_state() :: term().

-type property_value() :: undefined |
                          string() |
                          binary() |
                          number() |
                          boolean().
-type tagged_value() :: {uuid, binary()} |
                        {utf8, binary()} |
                        {binary, binary()} |
                        {boolean, boolean()} |
                        {double | float, float()} |
                        {long | int | short | byte, integer()} |
                        {ulong | uint | ushort | ubyte, non_neg_integer()} |
                        {timestamp, non_neg_integer()} |
                        {list, [tagged_value()]} |
                        {map, [{tagged_value(), tagged_value()}]} |
                        {array, atom(), [tagged_value()]} |
                        {as_is, TypeCode :: non_neg_integer(), binary()} |
                        null |
                        undefined.

%% behaviour callbacks for protocol specific implementation

%% protocol specific init function
%% returns a map of additional annotations to merge into the
%% protocol generic annotations map, e.g. ttl, priority and durable
-callback init(term()) ->
    {proto_state(), annotations()}.

%% the size of the payload and other meta data respectively
-callback size(proto_state()) ->
    {MetadataSize :: non_neg_integer(),
     PayloadSize :: non_neg_integer()}.

%% retrieve an x- header from the protocol data
%% the return value should be tagged with an AMQP 1.0 type
-callback x_header(binary(), proto_state()) ->
    tagged_value().

%% retrieve x- headers from the protocol data
%% the return values should be tagged with an AMQP 1.0 type
-callback x_headers(proto_state()) ->
    #{binary() => tagged_value()}.

%% retrieve a property field from the protocol data
%% e.g. message_id, correlation_id
-callback property(atom(), proto_state()) ->
    tagged_value().

%% return a map of header values used for message routing,
%% optionally include x- headers and / or complex types (i.e. tables, arrays etc)
-callback routing_headers(proto_state(), [x_headers | complex_types]) ->
    #{binary() => term()}.

%% Convert state to another protocol
%% all protocols must be able to convert to mc_amqp (AMQP 1.0)
-callback convert_to(Target :: protocol(), proto_state(), environment()) ->
    proto_state() | not_implemented.

%% Convert from another protocol
%% all protocols must be able to convert from mc_amqp (AMQP 1.0)
-callback convert_from(Source :: protocol(), proto_state(), environment()) ->
    proto_state() | not_implemented.

%% emit a protocol specific state package
%% typically used by connection / channel type process at consumer delivery
%% time
-callback protocol_state(proto_state(), annotations()) ->
    term().

%% prepare the data for either reading or storage
-callback prepare(read | store, proto_state()) ->
    proto_state().

%%% API

-spec init(protocol(), term(), annotations()) -> state().
init(Proto, Data, Anns) ->
    init(Proto, Data, Anns, #{}).

-spec init(protocol(), term(), annotations(), environment()) -> state().
init(Proto, Data, Anns0, Env) ->
    {ProtoData, ProtoAnns} = Proto:init(Data),
    Anns1 = case map_size(Env) of
                0 -> Anns0;
                _ -> Anns0#{env => Env}
            end,
    Anns2 = maps:merge(ProtoAnns, Anns1),
    Anns = ensure_received_at_timestamp(Anns2),
    #?MODULE{protocol = Proto,
             data = ProtoData,
             annotations = Anns}.

-spec size(state()) ->
    {MetadataSize :: non_neg_integer(),
     PayloadSize :: non_neg_integer()}.
size(#?MODULE{protocol = Proto,
              data = Data}) ->
    Proto:size(Data);
size(BasicMsg) ->
    mc_compat:size(BasicMsg).

-spec is(term()) -> boolean().
is(#?MODULE{}) ->
    true;
is(Term) ->
    mc_compat:is(Term).

-spec get_annotation(ann_key(), state()) -> ann_value() | undefined.
get_annotation(Key, #?MODULE{annotations = Anns}) ->
    maps:get(Key, Anns, undefined);
get_annotation(Key, BasicMessage) ->
    mc_compat:get_annotation(Key, BasicMessage).

-spec take_annotation(ann_key(), state()) -> {ann_value() | undefined, state()}.
take_annotation(Key, #?MODULE{annotations = Anns} = State) ->
    case maps:take(Key, Anns) of
        {Val, Anns1} ->
            {Val, State#?MODULE{annotations = Anns1}};
        error ->
            {undefined, State}
    end;
take_annotation(_Key, BasicMessage) ->
    {undefined, BasicMessage}.

-spec set_annotation(ann_key(), ann_value(), state()) ->
    state().
set_annotation(Key, Value, #?MODULE{annotations = Anns} = State) ->
    State#?MODULE{annotations = Anns#{Key => Value}};
set_annotation(Key, Value, BasicMessage) ->
    mc_compat:set_annotation(Key, Value, BasicMessage).

-spec x_header(Key :: binary(), state()) ->
    tagged_value().
x_header(Key, #?MODULE{protocol = Proto,
                       annotations = Anns,
                       data = Data}) ->
    %% x-headers may be have been added to the annotations map so
    %% we need to check that first
    case Anns of
        #{Key := Value} ->
            mc_util:infer_type(Value);
        _ ->
            %% if not we have to call into the protocol specific handler
            Proto:x_header(Key, Data)
    end;
x_header(Key, BasicMsg) ->
    mc_compat:x_header(Key, BasicMsg).

-spec x_headers(state()) ->
    #{binary() => tagged_value()}.
x_headers(#?MODULE{protocol = Proto,
                   annotations = Anns,
                   data = Data}) ->
    %% x-headers may be have been added to the annotations map.
    New = maps:filtermap(
            fun(Key, Val) ->
                    case mc_util:is_x_header(Key) of
                        true ->
                            {true, mc_util:infer_type(Val)};
                        false ->
                            false
                    end
            end, Anns),
    maps:merge(Proto:x_headers(Data), New);
x_headers(BasicMsg) ->
    mc_compat:x_headers(BasicMsg).

-spec routing_headers(state(), [x_headers | complex_types]) ->
    #{binary() => property_value()}.
routing_headers(#?MODULE{protocol = Proto,
                         annotations = Anns,
                         data = Data}, Options) ->
    New = case lists:member(x_headers, Options) of
              true ->
                  maps:filter(fun (Key, _) ->
                                      mc_util:is_x_header(Key)
                              end, Anns);
              false ->
                  #{}
          end,
    maps:merge(Proto:routing_headers(Data, Options), New);
routing_headers(BasicMsg, Opts) ->
    mc_compat:routing_headers(BasicMsg, Opts).

-spec exchange(state()) -> undefined | rabbit_misc:resource_name().
exchange(#?MODULE{annotations = Anns}) ->
    maps:get(?ANN_EXCHANGE, Anns, undefined);
exchange(BasicMessage) ->
    mc_compat:get_annotation(?ANN_EXCHANGE, BasicMessage).

-spec routing_keys(state()) -> [rabbit_types:routing_key()].
routing_keys(#?MODULE{annotations = Anns}) ->
    maps:get(?ANN_ROUTING_KEYS, Anns, []);
routing_keys(BasicMessage) ->
    mc_compat:get_annotation(?ANN_ROUTING_KEYS, BasicMessage).

-spec is_persistent(state()) -> boolean().
is_persistent(#?MODULE{annotations = Anns}) ->
    maps:get(?ANN_DURABLE, Anns, true);
is_persistent(BasicMsg) ->
    mc_compat:is_persistent(BasicMsg).

-spec ttl(state()) -> undefined | non_neg_integer().
ttl(#?MODULE{annotations = Anns}) ->
    maps:get(ttl, Anns, undefined);
ttl(BasicMsg) ->
    mc_compat:ttl(BasicMsg).

-spec timestamp(state()) -> undefined | non_neg_integer().
timestamp(#?MODULE{annotations = Anns}) ->
    maps:get(?ANN_TIMESTAMP, Anns, undefined);
timestamp(BasicMsg) ->
    mc_compat:timestamp(BasicMsg).

-spec priority(state()) -> undefined | non_neg_integer().
priority(#?MODULE{annotations = Anns}) ->
    maps:get(?ANN_PRIORITY, Anns, undefined);
priority(BasicMsg) ->
    mc_compat:priority(BasicMsg).

-spec correlation_id(state()) ->
    {uuid, binary()} |
    {utf8, binary()} |
    {binary, binary()} |
    {ulong, non_neg_integer()} |
    undefined.
correlation_id(#?MODULE{protocol = Proto,
                        data = Data}) ->
    Proto:property(?FUNCTION_NAME, Data);
correlation_id(BasicMsg) ->
    mc_compat:correlation_id(BasicMsg).

-spec user_id(state()) ->
    {binary, rabbit_types:username()} |
    undefined.
user_id(#?MODULE{protocol = Proto,
                 data = Data}) ->
    Proto:property(?FUNCTION_NAME, Data);
user_id(BasicMsg) ->
    mc_compat:user_id(BasicMsg).

-spec message_id(state()) ->
    {uuid, binary()} |
    {utf8, binary()} |
    {binary, binary()} |
    {ulong, non_neg_integer()} |
    undefined.
message_id(#?MODULE{protocol = Proto,
                    data = Data}) ->
    Proto:property(?FUNCTION_NAME, Data);
message_id(BasicMsg) ->
    mc_compat:message_id(BasicMsg).

-spec property(atom(), state()) ->
    tagged_value().
property(Property, #?MODULE{protocol = Proto,
                            data = Data}) ->
    Proto:property(Property, Data);
property(_Property, _BasicMsg) ->
    undefined.

-spec set_ttl(undefined | non_neg_integer(), state()) -> state().
set_ttl(Value, #?MODULE{annotations = Anns} = State) ->
    State#?MODULE{annotations = Anns#{ttl => Value}};
set_ttl(Value, BasicMsg) ->
    mc_compat:set_ttl(Value, BasicMsg).

-spec convert(protocol(), state()) -> state().
convert(Proto, State) ->
    convert(Proto, State, #{}).

-spec convert(protocol(), state(), environment()) -> state().
convert(Proto, #?MODULE{protocol = Proto} = State, _Env) ->
    State;
convert(TargetProto, #?MODULE{protocol = SourceProto,
                              annotations = Anns,
                              data = Data0} = State,
        TargetEnv) ->
    Data = SourceProto:prepare(read, Data0),
    SourceEnv = maps:get(env, Anns, #{}),
    Env = maps:merge(SourceEnv, TargetEnv),
    TargetState =
        case SourceProto:convert_to(TargetProto, Data, Env) of
            not_implemented ->
                case TargetProto:convert_from(SourceProto, Data, Env) of
                    not_implemented ->
                        AmqpData = SourceProto:convert_to(mc_amqp, Data, Env),
                        mc_amqp:convert_to(TargetProto, AmqpData, Env);
                    TargetState0 ->
                        TargetState0
                end;
            TargetState0 ->
                TargetState0
        end,
    State#?MODULE{protocol = TargetProto,
                  data = TargetState};
convert(Proto, BasicMsg, _Env) ->
    mc_compat:convert_to(Proto, BasicMsg).

-spec protocol_state(state()) -> term().
protocol_state(#?MODULE{protocol = Proto,
                        annotations = Anns,
                        data = Data}) ->
    Proto:protocol_state(Data, Anns);
protocol_state(BasicMsg) ->
    mc_compat:protocol_state(BasicMsg).

-spec record_death(rabbit_dead_letter:reason(),
                   rabbit_misc:resource_name(),
                   state(),
                   environment()) -> state().
record_death(Reason, SourceQueue,
             #?MODULE{annotations = Anns0} = State,
             Env)
  when is_atom(Reason) andalso
       is_binary(SourceQueue) ->
    Key = {SourceQueue, Reason},
    #{?ANN_EXCHANGE := Exchange,
      ?ANN_ROUTING_KEYS := RKeys0} = Anns0,
    %% The routing keys that we record in the death history and will
    %% report to the client should include CC, but exclude BCC.
    RKeys = case Anns0 of
                #{bcc := BccKeys} ->
                    RKeys0 -- BccKeys;
                _ ->
                    RKeys0
            end,
    Timestamp = os:system_time(millisecond),
    Ttl = maps:get(ttl, Anns0, undefined),
    DeathAnns = rabbit_misc:maps_put_truthy(
                  ttl, Ttl, #{first_time => Timestamp,
                              last_time => Timestamp}),
    NewDeath = #death{exchange = Exchange,
                      routing_keys = RKeys,
                      count = 1,
                      anns = DeathAnns},
    ReasonBin = atom_to_binary(Reason),
    Anns = case Anns0 of
               #{deaths := Deaths0} ->
                   Deaths = case Deaths0 of
                                #deaths{records = Rs0} ->
                                    Rs = maps:update_with(
                                           Key,
                                           fun(Death) ->
                                                   update_death(Death, Timestamp)
                                           end,
                                           NewDeath,
                                           Rs0),
                                    Deaths0#deaths{last = Key,
                                                   records = Rs};
                                _ ->
                                    %% Deaths are ordered by recency
                                    case lists:keytake(Key, 1, Deaths0) of
                                        {value, {Key, D0}, Deaths1} ->
                                            D = update_death(D0, Timestamp),
                                            [{Key, D} | Deaths1];
                                        false ->
                                            [{Key, NewDeath} | Deaths0]
                                    end
                            end,
                   Anns0#{<<"x-last-death-reason">> => ReasonBin,
                          <<"x-last-death-queue">> => SourceQueue,
                          <<"x-last-death-exchange">> => Exchange,
                          deaths := Deaths};
               _ ->
                   Deaths = case Env of
                                #{?FF_MC_DEATHS_V2 := false} ->
                                    #deaths{last = Key,
                                            first = Key,
                                            records = #{Key => NewDeath}};
                                _ ->
                                    [{Key, NewDeath}]
                            end,
                   Anns0#{<<"x-first-death-reason">> => ReasonBin,
                          <<"x-first-death-queue">> => SourceQueue,
                          <<"x-first-death-exchange">> => Exchange,
                          <<"x-last-death-reason">> => ReasonBin,
                          <<"x-last-death-queue">> => SourceQueue,
                          <<"x-last-death-exchange">> => Exchange,
                          deaths => Deaths}
           end,
    State#?MODULE{annotations = Anns};
record_death(Reason, SourceQueue, BasicMsg, Env) ->
    mc_compat:record_death(Reason, SourceQueue, BasicMsg, Env).

update_death(#death{count = Count,
                    anns = DeathAnns} = Death, Timestamp) ->
    Death#death{count = Count + 1,
                anns = DeathAnns#{last_time := Timestamp}}.

-spec is_death_cycle(rabbit_misc:resource_name(), state()) -> boolean().
is_death_cycle(TargetQueue, #?MODULE{annotations = #{deaths := #deaths{records = Rs}}}) ->
    is_cycle_v1(TargetQueue, maps:keys(Rs));
is_death_cycle(TargetQueue, #?MODULE{annotations = #{deaths := Deaths}}) ->
    is_cycle_v2(TargetQueue, Deaths);
is_death_cycle(_TargetQueue, #?MODULE{}) ->
    false;
is_death_cycle(TargetQueue, BasicMsg) ->
    mc_compat:is_death_cycle(TargetQueue, BasicMsg).

%% Returns death queue names ordered by recency.
-spec death_queue_names(state()) -> [rabbit_misc:resource_name()].
death_queue_names(#?MODULE{annotations = #{deaths := #deaths{records = Rs}}}) ->
    proplists:get_keys(maps:keys(Rs));
death_queue_names(#?MODULE{annotations = #{deaths := Deaths}}) ->
    lists:map(fun({{Queue, _Reason}, _Death}) ->
                      Queue
              end, Deaths);
death_queue_names(#?MODULE{}) ->
    [];
death_queue_names(BasicMsg) ->
    mc_compat:death_queue_names(BasicMsg).

-spec prepare(read | store, state()) -> state().
prepare(For, #?MODULE{protocol = Proto,
                      data = Data} = State) ->
    State#?MODULE{data = Proto:prepare(For, Data)};
prepare(For, State) ->
    mc_compat:prepare(For, State).

%% INTERNAL

is_cycle_v2(TargetQueue, Deaths) ->
    case lists:splitwith(fun({{SourceQueue, _Reason}, #death{}}) ->
                                 SourceQueue =/= TargetQueue
                         end, Deaths) of
        {_, []} ->
            false;
        {L, [H | _]} ->
            %% There is a cycle, but we only want to drop the message
            %% if the cycle is "fully automatic", i.e. without a client
            %% expliclity rejecting the message somewhere in the cycle.
            lists:all(fun({{_SourceQueue, Reason}, _Death}) ->
                              Reason =/= rejected
                      end, [H | L])
    end.

%% The desired v1 behaviour is the following:
%% "If there is a death with a source queue that is the same as the target
%% queue name and there are no newer deaths with the 'rejected' reason then
%% consider this a cycle."
%% However, the correct death order cannot be reliably determined in v1.
%% v2 fixes this bug.
is_cycle_v1(_Queue, []) ->
    false;
is_cycle_v1(_Queue, [{_Q, rejected} | _]) ->
    %% any rejection breaks the cycle
    false;
is_cycle_v1(Queue, [{Queue, Reason} | _])
  when Reason =/= rejected ->
    true;
is_cycle_v1(Queue, [_ | Rem]) ->
    is_cycle_v1(Queue, Rem).

ensure_received_at_timestamp(Anns)
  when is_map_key(?ANN_RECEIVED_AT_TIMESTAMP, Anns) ->
    Anns;
ensure_received_at_timestamp(Anns) ->
    Millis = os:system_time(millisecond),
    Anns#{?ANN_RECEIVED_AT_TIMESTAMP => Millis}.
