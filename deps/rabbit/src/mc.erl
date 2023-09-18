-module(mc).

-export([
         init/3,
         size/1,
         is/1,
         get_annotation/2,
         take_annotation/2,
         set_annotation/3,
         %% properties
         is_persistent/1,
         ttl/1,
         correlation_id/1,
         message_id/1,
         timestamp/1,
         priority/1,
         set_ttl/2,
         x_header/2,
         routing_headers/2,
         %%
         convert/2,
         protocol_state/1,
         prepare/2,
         record_death/3,
         is_death_cycle/2,
         last_death/1,
         death_queue_names/1
        ]).

-include("mc.hrl").

-type str() :: atom() | string() | binary().
-type internal_ann_key() :: atom().
-type x_ann_key() :: binary(). %% should begin with x- or ideally x-opt-
-type x_ann_value() :: str() | integer() | float() | [x_ann_value()].
-type protocol() :: module().
-type annotations() :: #{internal_ann_key() => term(),
                         x_ann_key() => x_ann_value()}.
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
              ann_value/0
             ]).

-type proto_state() :: term().

-type property_value() :: undefined |
                          string() |
                          binary() |
                          integer() |
                          float() |
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

%% retrieve and x- header from the protocol data
%% the return value should be tagged with an AMQP 1.0 type
-callback x_header(binary(), proto_state()) ->
    tagged_value().

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
-callback convert_to(Target :: protocol(), proto_state()) ->
    proto_state() | not_implemented.

%% Convert from another protocol
%% all protocols must be able to convert from mc_amqp (AMQP 1.0)
-callback convert_from(Source :: protocol(), proto_state()) ->
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
init(Proto, Data, Anns)
  when is_atom(Proto)
       andalso is_map(Anns) ->
    {ProtoData, ProtoAnns} = Proto:init(Data),
    #?MODULE{protocol = Proto,
             data = ProtoData,
             annotations = maps:merge(ProtoAnns, Anns)}.

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
    State#?MODULE{annotations = maps:put(Key, Value, Anns)};
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

-spec is_persistent(state()) -> boolean().
is_persistent(#?MODULE{annotations = Anns}) ->
    maps:get(durable, Anns, true);
is_persistent(BasicMsg) ->
    mc_compat:is_persistent(BasicMsg).

-spec ttl(state()) -> undefined | non_neg_integer().
ttl(#?MODULE{annotations = Anns}) ->
    maps:get(ttl, Anns, undefined);
ttl(BasicMsg) ->
    mc_compat:ttl(BasicMsg).


-spec timestamp(state()) -> undefined | non_neg_integer().
timestamp(#?MODULE{annotations = Anns}) ->
    maps:get(timestamp, Anns, undefined);
timestamp(BasicMsg) ->
    mc_compat:timestamp(BasicMsg).

-spec priority(state()) -> undefined | non_neg_integer().
priority(#?MODULE{annotations = Anns}) ->
    maps:get(priority, Anns, undefined);
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

-spec set_ttl(undefined | non_neg_integer(), state()) -> state().
set_ttl(Value, #?MODULE{annotations = Anns} = State) ->
    State#?MODULE{annotations = maps:put(ttl, Value, Anns)};
set_ttl(Value, BasicMsg) ->
    mc_compat:set_ttl(Value, BasicMsg).

-spec convert(protocol(), state()) -> state().
convert(Proto, #?MODULE{protocol = Proto} = State) ->
    State;
convert(TargetProto, #?MODULE{protocol = SourceProto,
                              data = Data0} = State) ->
    Data = SourceProto:prepare(read, Data0),
    TargetState =
        case SourceProto:convert_to(TargetProto, Data) of
            not_implemented ->
                case TargetProto:convert_from(SourceProto, Data) of
                    not_implemented ->
                        AmqpData = SourceProto:convert_to(mc_amqp, Data),
                        mc_amqp:convert_to(TargetProto, AmqpData);
                    TargetState0 ->
                        TargetState0
                end;
            TargetState0 ->
                TargetState0
        end,
    State#?MODULE{protocol = TargetProto,
                  data = TargetState};
convert(Proto, BasicMsg) ->
    mc_compat:convert_to(Proto, BasicMsg).

-spec protocol_state(state()) -> term().
protocol_state(#?MODULE{protocol = Proto,
                        annotations = Anns,
                        data = Data}) ->
    Proto:protocol_state(Data, Anns);
protocol_state(BasicMsg) ->
    mc_compat:protocol_state(BasicMsg).

-spec record_death(rabbit_dead_letter:reason(),
                   SourceQueue :: rabbit_misc:resource_name(),
                   state()) -> state().
record_death(Reason, SourceQueue,
             #?MODULE{protocol = _Mod,
                      data = _Data,
                      annotations = Anns0} = State)
  when is_atom(Reason) andalso is_binary(SourceQueue) ->
    Key = {SourceQueue, Reason},
    Exchange = maps:get(exchange, Anns0),
    RoutingKeys = maps:get(routing_keys, Anns0),
    Timestamp = os:system_time(millisecond),
    Ttl = maps:get(ttl, Anns0, undefined),

    ReasonBin = atom_to_binary(Reason),
    DeathAnns = rabbit_misc:maps_put_truthy(ttl, Ttl, #{first_time => Timestamp,
                                                        last_time => Timestamp}),
    case maps:get(deaths, Anns0, undefined) of
        undefined ->
            Ds = #deaths{last = Key,
                         first = Key,
                         records = #{Key => #death{count = 1,
                                                   exchange = Exchange,
                                                   routing_keys = RoutingKeys,
                                                   anns = DeathAnns}}},
            Anns = Anns0#{<<"x-first-death-reason">> => ReasonBin,
                          <<"x-first-death-queue">> => SourceQueue,
                          <<"x-first-death-exchange">> => Exchange,
                          <<"x-last-death-reason">> => ReasonBin,
                          <<"x-last-death-queue">> => SourceQueue,
                          <<"x-last-death-exchange">> => Exchange
                         },

            State#?MODULE{annotations = Anns#{deaths => Ds}};
        #deaths{records = Rs} = Ds0 ->
            Death = #death{count = C,
                           anns = DA} = maps:get(Key, Rs,
                                                 #death{exchange = Exchange,
                                                        routing_keys = RoutingKeys,
                                                        anns = DeathAnns}),
            Ds = Ds0#deaths{last = Key,
                            records = Rs#{Key =>
                                          Death#death{count = C + 1,
                                                      anns = DA#{last_time => Timestamp}}}},
            Anns = Anns0#{deaths => Ds,
                          <<"x-last-death-reason">> => ReasonBin,
                          <<"x-last-death-queue">> => SourceQueue,
                          <<"x-last-death-exchange">> => Exchange},
            State#?MODULE{annotations = Anns}
    end;
record_death(Reason, SourceQueue, BasicMsg) ->
    mc_compat:record_death(Reason, SourceQueue, BasicMsg).


-spec is_death_cycle(rabbit_misc:resource_name(), state()) -> boolean().
is_death_cycle(TargetQueue, #?MODULE{annotations = #{deaths := Deaths}}) ->
    is_cycle(TargetQueue, maps:keys(Deaths#deaths.records));
is_death_cycle(_TargetQueue, #?MODULE{}) ->
    false;
is_death_cycle(TargetQueue, BasicMsg) ->
    mc_compat:is_death_cycle(TargetQueue, BasicMsg).

-spec death_queue_names(state()) -> [rabbit_misc:resource_name()].
death_queue_names(#?MODULE{annotations = Anns}) ->
    case maps:get(deaths, Anns, undefined) of
        undefined ->
            [];
        #deaths{records = Records} ->
            proplists:get_keys(maps:keys(Records))
    end;
death_queue_names(BasicMsg) ->
    mc_compat:death_queue_names(BasicMsg).

-spec last_death(state()) ->
    undefined | {death_key(), #death{}}.
last_death(#?MODULE{annotations = Anns})
  when not is_map_key(deaths, Anns) ->
    undefined;
last_death(#?MODULE{annotations = #{deaths := #deaths{last = Last,
                                                      records = Rs}}}) ->
    {Last, maps:get(Last, Rs)};
last_death(BasicMsg) ->
    mc_compat:last_death(BasicMsg).

-spec prepare(read | store, state()) -> state().
prepare(For, #?MODULE{protocol = Proto,
                      data = Data} = State) ->
    State#?MODULE{data = Proto:prepare(For, Data)};
prepare(For, State) ->
    mc_compat:prepare(For, State).

%% INTERNAL

%% if there is a death with a source queue that is the same as the target
%% queue name and there are no newer deaths with the 'rejected' reason then
%% consider this a cycle
is_cycle(_Queue, []) ->
    false;
is_cycle(_Queue, [{_Q, rejected} | _]) ->
    %% any rejection breaks the cycle
    false;
is_cycle(Queue, [{Queue, Reason} | _])
  when Reason =/= rejected ->
    true;
is_cycle(Queue, [_ | Rem]) ->
    is_cycle(Queue, Rem).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
