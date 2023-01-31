-module(rabbit_msg_record).

-export([
         init/1,
         to_iodata/1,
         from_amqp091/2,
         to_amqp091/1,
         add_message_annotations/2,
         message_annotation/2,
         message_annotation/3,
         from_091/2,
         to_091/2
         ]).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-type maybe(T) :: T | undefined.
-type amqp10_data() :: #'v1_0.data'{} |
                       [#'v1_0.amqp_sequence'{} | #'v1_0.data'{}] |
                       #'v1_0.amqp_value'{}.
-record(msg,
        {
         % header :: maybe(#'v1_0.header'{}),
         % delivery_annotations :: maybe(#'v1_0.delivery_annotations'{}),
         message_annotations :: maybe(#'v1_0.message_annotations'{}) | iodata(),
         properties :: maybe(#'v1_0.properties'{}) | iodata(),
         application_properties :: maybe(#'v1_0.application_properties'{}) | iodata(),
         data :: maybe(amqp10_data()) | iodata()
         % footer :: maybe(#'v1_0.footer'{})
         }).

%% holds static or rarely changing fields
-record(cfg, {}).
-record(?MODULE, {cfg :: #cfg{},
                  msg :: #msg{}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-define(AMQP10_TYPE, <<"amqp-1.0">>).
-define(AMQP10_PROPERTIES_HEADER, <<"x-amqp-1.0-properties">>).
-define(AMQP10_APP_PROPERTIES_HEADER, <<"x-amqp-1.0-app-properties">>).
-define(AMQP10_MESSAGE_ANNOTATIONS_HEADER, <<"x-amqp-1.0-message-annotations">>).

%% this module acts as a wrapper / converter for the internal binary storage format
%% (AMQP 1.0) and any format it needs to be converted to / from.
%% Efficiency is key. No unnecessary allocations or work should be done until it
%% is absolutely needed

%% init from an AMQP 1.0 encoded binary
-spec init(binary()) -> state().
init(Bin) when is_binary(Bin) ->
    %% TODO: delay parsing until needed
    {MA, P, AP, D0} = decode(amqp10_framing:decode_bin(Bin),
                             {undefined, undefined, undefined, undefined}),

    D1 = case D0 of
             Sections when is_list(D0) ->
                 lists:reverse(Sections);
             _ ->
                 D0
         end,

    #?MODULE{cfg = #cfg{},
             msg = #msg{properties = P,
                        application_properties = AP,
                        message_annotations = MA,
                        data = D1}}.

decode([], Acc) ->
    Acc;
decode([#'v1_0.message_annotations'{} = MA | Rem], {_, P, AP, D}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.properties'{} = P | Rem], {MA, _, AP, D}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.application_properties'{} = AP | Rem], {MA, P, _, D}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.amqp_value'{} = D | Rem], {MA, P, AP, _}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.data'{} = D | Rem], {MA, P, AP, undefined}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.data'{} = D | Rem], {MA, P, AP, B}) when is_list(B) ->
    decode(Rem, {MA, P, AP, [D | B]});
decode([#'v1_0.data'{} = D | Rem], {MA, P, AP, B}) ->
    decode(Rem, {MA, P, AP, [D, B]});
decode([#'v1_0.amqp_sequence'{} = D | Rem], {MA, P, AP, undefined}) ->
    decode(Rem, {MA, P, AP, [D]});
decode([#'v1_0.amqp_sequence'{} = D | Rem], {MA, P, AP, B}) when is_list(B) ->
    decode(Rem, {MA, P, AP, [D | B]}).


amqp10_properties_empty(#'v1_0.properties'{message_id = undefined,
                                           user_id = undefined,
                                           to = undefined,
                                           reply_to = undefined,
                                           correlation_id = undefined,
                                           content_type = undefined,
                                           content_encoding = undefined,
                                           creation_time = undefined}) ->
    true;
amqp10_properties_empty(_) ->
    false.

%% to realise the final binary data representation
-spec to_iodata(state()) -> iodata().
to_iodata(#?MODULE{msg = #msg{properties = P,
                              application_properties = AP,
                              message_annotations = MA,
                              data = Data}}) ->
    [
     case MA of
         #'v1_0.message_annotations'{content = []} ->
             <<>>;
         #'v1_0.message_annotations'{} ->
             amqp10_framing:encode_bin(MA);
         MsgAnnotBin ->
             MsgAnnotBin
     end,
     case P of
         #'v1_0.properties'{} ->
             case amqp10_properties_empty(P) of
                 true -> <<>>;
                 false ->
                     amqp10_framing:encode_bin(P)
             end;
         PropsBin ->
             PropsBin
     end,
     case AP of
         #'v1_0.application_properties'{content = []} ->
             <<>>;
         #'v1_0.application_properties'{} ->
             amqp10_framing:encode_bin(AP);
         AppPropsBin ->
             AppPropsBin
     end,
     case Data of
         DataBin when is_binary(Data) orelse is_list(Data) ->
             DataBin;
         _ ->
             amqp10_framing:encode_bin(Data)
     end
    ].

%% TODO: refine type spec here
-spec add_message_annotations(#{binary() => {atom(), term()}}, state()) ->
    state().
add_message_annotations(Anns,
                        #?MODULE{msg =
                                 #msg{message_annotations = undefined} = Msg} = State) ->
    add_message_annotations(Anns,
                            State#?MODULE{msg = Msg#msg{message_annotations =
                                                        #'v1_0.message_annotations'{content = []}}});
add_message_annotations(Anns,
                        #?MODULE{msg =
                                 #msg{message_annotations =
                                      #'v1_0.message_annotations'{content = C}} = Msg} = State) ->
    Content = maps:fold(
                fun (K, {T, V}, Acc) ->
                        map_add(symbol, K, T, V, Acc)
                end,
                C,
                Anns),

    State#?MODULE{msg =
                  Msg#msg{message_annotations =
                            #'v1_0.message_annotations'{content = Content}}};
add_message_annotations(Anns,
                        #?MODULE{msg =
                                 #msg{message_annotations = MABin} = Msg} = State0) ->
    [MA] = amqp10_framing:decode_bin(iolist_to_binary(MABin)),
    State1 = State0#?MODULE{msg =
                            Msg#msg{message_annotations = MA}},
    add_message_annotations(Anns, State1).

%% TODO: refine
-type amqp10_term() :: {atom(), term()}.

-spec message_annotation(binary(), state()) -> undefined | amqp10_term().
message_annotation(Key, State) ->
    message_annotation(Key, State, undefined).

-spec message_annotation(binary(), state(), undefined | amqp10_term()) ->
    undefined | amqp10_term().
message_annotation(_Key, #?MODULE{msg = #msg{message_annotations = undefined}},
                  Default) ->
    Default;
message_annotation(Key,
                   #?MODULE{msg =
                            #msg{message_annotations =
                                 #'v1_0.message_annotations'{content = Content}}},
                  Default)
  when is_binary(Key) ->
    case lists:search(fun ({{symbol, K}, _}) -> K == Key end, Content) of
        {value, {_K, V}} ->
            V;
        false ->
            Default
    end.


%% take a binary AMQP 1.0 input function,
%% parses it and returns the current parse state
%% this is the input function from storage and from, e.g. socket input
-spec from_amqp091(#'P_basic'{}, iodata()) -> state().
from_amqp091(#'P_basic'{type = T} = PB, Data) ->
    MA = from_amqp091_to_amqp10_message_annotations(PB),
    P = from_amqp091_to_amqp10_properties(PB),
    AP = from_amqp091_to_amqp10_app_properties(PB),

    D = case T of
            ?AMQP10_TYPE ->
                %% the body is already AMQP 1.0 binary content, so leaving it as-is
                Data;
            _ ->
                #'v1_0.data'{content = Data}
        end,

    #?MODULE{cfg = #cfg{},
             msg = #msg{properties = P,
                        application_properties = AP,
                        message_annotations = MA,
                        data = D}}.

from_amqp091_to_amqp10_properties(#'P_basic'{headers = Headers} = P) when is_list(Headers) ->
    case proplists:lookup(?AMQP10_PROPERTIES_HEADER, Headers) of
        none ->
            convert_amqp091_to_amqp10_properties(P);
        {_, _, PropsBin} ->
            PropsBin
    end;
from_amqp091_to_amqp10_properties(P) ->
    convert_amqp091_to_amqp10_properties(P).

convert_amqp091_to_amqp10_properties(#'P_basic'{message_id = MsgId,
                                                user_id = UserId,
                                                reply_to = ReplyTo,
                                                correlation_id = CorrId,
                                                content_type = ContentType,
                                                content_encoding = ContentEncoding,
                                                timestamp = Timestamp
                                               }) ->
    ConvertedTs = case Timestamp of
                      undefined ->
                          undefined;
                      _ ->
                          Timestamp * 1000
                  end,
    #'v1_0.properties'{message_id = wrap(utf8, MsgId),
                       user_id = wrap(binary, UserId),
                       to = undefined,
                       reply_to = wrap(utf8, ReplyTo),
                       correlation_id = wrap(utf8, CorrId),
                       content_type = wrap(symbol, ContentType),
                       content_encoding = wrap(symbol, ContentEncoding),
                       creation_time = wrap(timestamp, ConvertedTs)}.

from_amqp091_to_amqp10_app_properties(#'P_basic'{headers = Headers} = P)
  when is_list(Headers) ->
    case proplists:lookup(?AMQP10_APP_PROPERTIES_HEADER, Headers) of
        none ->
            convert_amqp091_to_amqp10_app_properties(P);
        {_, _, AppPropsBin} ->
            AppPropsBin
    end;
from_amqp091_to_amqp10_app_properties(P) ->
    convert_amqp091_to_amqp10_app_properties(P).

convert_amqp091_to_amqp10_app_properties(#'P_basic'{headers = Headers,
                                                    type = Type,
                                                    app_id = AppId}) ->
    APC0 = [{wrap(utf8, K), from_091(T, V)} || {K, T, V}
                                               <- case Headers of
                                                      undefined -> [];
                                                      _ -> Headers
                                                  end, not unsupported_header_value_type(T),
                                               not filtered_header(K)],

    APC1 = case Type of
               ?AMQP10_TYPE ->
                   %% no need to modify the application properties for the type
                   %% this info will be restored on decoding if necessary
                   APC0;
               _ ->
                   map_add(utf8, <<"x-basic-type">>, utf8, Type, APC0)
           end,

    %% properties that do not map directly to AMQP 1.0 properties are stored
    %% in application properties
    APC2 = map_add(utf8, <<"x-basic-app-id">>, utf8, AppId, APC1),
    #'v1_0.application_properties'{content = APC2}.

from_amqp091_to_amqp10_message_annotations(#'P_basic'{headers = Headers} = P) when is_list(Headers) ->
    case proplists:lookup(?AMQP10_MESSAGE_ANNOTATIONS_HEADER, Headers) of
        none ->
            convert_amqp091_to_amqp10_message_annotations(P);
        {_, _, MessageAnnotationsBin} ->
            MessageAnnotationsBin
    end;
from_amqp091_to_amqp10_message_annotations(P) ->
    convert_amqp091_to_amqp10_message_annotations(P).

convert_amqp091_to_amqp10_message_annotations(#'P_basic'{priority = Priority,
                                                         delivery_mode = DelMode,
                                                         expiration = Expiration}) ->
    MAC = map_add(symbol, <<"x-basic-priority">>, ubyte, Priority,
                  map_add(symbol, <<"x-basic-delivery-mode">>, ubyte, DelMode,
                          map_add(symbol, <<"x-basic-expiration">>, utf8, Expiration, []))),

    #'v1_0.message_annotations'{content = MAC}.

map_add(_T, _Key, _Type, undefined, Acc) ->
    Acc;
map_add(KeyType, Key, Type, Value, Acc) ->
    [{wrap(KeyType, Key), wrap(Type, Value)} | Acc].

-spec to_amqp091(state()) -> {#'P_basic'{}, iodata()}.
to_amqp091(#?MODULE{msg = #msg{properties = P,
                               application_properties = APR,
                               message_annotations = MAR,
                               data = Data}}) ->

    %% anything else than a single data section is expected to be AMQP 1.0 binary content
    %% enforcing this convention
    {Payload, IsAmqp10} =  case Data of
                               undefined ->
                                   %% not an expected value,
                                   %% but handling it with an empty binary anyway
                                   {<<>>, false};
                               #'v1_0.data'{content = C} ->
                                   {C, false};
                               Sections when is_list(Data)->
                                   B = [amqp10_framing:encode_bin(S) || S <- Sections],
                                   {iolist_to_binary(B),
                                    true};
                               V ->
                                   {iolist_to_binary(amqp10_framing:encode_bin(V)), true}
                           end,

    #'v1_0.properties'{message_id = MsgId,
                       user_id = UserId,
                       reply_to = ReplyTo0,
                       correlation_id = CorrId,
                       content_type = ContentType,
                       content_encoding = ContentEncoding,
                       creation_time = Timestamp} = case P of
                                                        undefined ->
                                                            #'v1_0.properties'{};
                                                        _ ->
                                                            P
                                                    end,

    AP0 = case APR of
              #'v1_0.application_properties'{content = AC} -> AC;
              _ -> []
          end,
    MA0 = case MAR of
              #'v1_0.message_annotations'{content = MC} -> MC;
              _ -> []
          end,

    {Type, AP1} = case {amqp10_map_get(utf8(<<"x-basic-type">>), AP0), IsAmqp10} of
                      {{undefined, M}, true} ->
                          {?AMQP10_TYPE, M};
                      {{T, M}, _} ->
                          {T, M}
                  end,
    {AppId, AP} = amqp10_map_get(utf8(<<"x-basic-app-id">>), AP1),

    {Priority, MA1} = amqp10_map_get(symbol(<<"x-basic-priority">>), MA0),
    {DelMode, MA2} = amqp10_map_get(symbol(<<"x-basic-delivery-mode">>), MA1),
    {Expiration, _MA} = amqp10_map_get(symbol(<<"x-basic-expiration">>), MA2),

    Headers0 = [to_091(unwrap(K), V) || {K, V} <- AP],
    {Headers1, MsgId091} = message_id(MsgId, <<"x-message-id-type">>, Headers0),
    {Headers, CorrId091} = message_id(CorrId, <<"x-correlation-id-type">>, Headers1),

    BP = #'P_basic'{message_id =  MsgId091,
                    delivery_mode = DelMode,
                    expiration = Expiration,
                    user_id = unwrap(UserId),
                    headers = case Headers of
                                  [] -> undefined;
                                  _ -> Headers
                              end,
                    reply_to = unwrap(ReplyTo0),
                    type = Type,
                    app_id = AppId,
                    priority = Priority,
                    correlation_id = CorrId091,
                    content_type = unwrap(ContentType),
                    content_encoding = unwrap(ContentEncoding),
                    timestamp = case unwrap(Timestamp) of
                                    undefined ->
                                        undefined;
                                    Ts ->
                                        Ts  div 1000
                                end
                   },
    {BP, Payload}.

%%% Internal

amqp10_map_get(K, AP0) ->
    case lists:keytake(K, 1, AP0) of
        false ->
            {undefined, AP0};
        {value, {_, V}, AP}  ->
            {unwrap(V), AP}
    end.

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.

unwrap(undefined) ->
    undefined;
unwrap({_Type, V}) ->
    V.

% symbol_for(#'v1_0.properties'{}) ->
%     {symbol, <<"amqp:properties:list">>};

% number_for(#'v1_0.properties'{}) ->
%     {ulong, 115};
% encode(Frame = #'v1_0.properties'{}) ->
%     amqp10_framing:encode_described(list, 115, Frame);

% encode_described(list, CodeNumber, Frame) ->
%     {described, {ulong, CodeNumber},
%      {list, lists:map(fun encode/1, tl(tuple_to_list(Frame)))}};

% -spec generate(amqp10_type()) -> iolist().
% generate({described, Descriptor, Value}) ->
%     DescBin = generate(Descriptor),
%     ValueBin = generate(Value),
%     [ ?DESCRIBED_BIN, DescBin, ValueBin ].

to_091(Key, {utf8, V}) when is_binary(V) -> {Key, longstr, V};
to_091(Key, {long, V}) -> {Key, long, V};
to_091(Key, {byte, V}) -> {Key, byte, V};
to_091(Key, {ubyte, V}) -> {Key, unsignedbyte, V};
to_091(Key, {short, V}) -> {Key, short, V};
to_091(Key, {ushort, V}) -> {Key, unsignedshort, V};
to_091(Key, {uint, V}) -> {Key, unsignedint, V};
to_091(Key, {int, V}) -> {Key, signedint, V};
to_091(Key, {double, V}) -> {Key, double, V};
to_091(Key, {float, V}) -> {Key, float, V};
%% NB: header values can never be shortstr!
to_091(Key, {timestamp, V}) -> {Key, timestamp, V div 1000};
to_091(Key, {binary, V}) -> {Key, binary, V};
to_091(Key, {boolean, V}) -> {Key, bool, V};
to_091(Key, true) -> {Key, bool, true};
to_091(Key, false) -> {Key, bool, false};
%% TODO
to_091(Key, undefined) -> {Key, void, undefined};
to_091(Key, null) -> {Key, void, undefined}.

from_091(longstr, V) when is_binary(V) -> {utf8, V};
from_091(long, V) -> {long, V};
from_091(unsignedbyte, V) -> {ubyte, V};
from_091(short, V) -> {short, V};
from_091(unsignedshort, V) -> {ushort, V};
from_091(unsignedint, V) -> {uint, V};
from_091(signedint, V) -> {int, V};
from_091(double, V) -> {double, V};
from_091(float, V) -> {float, V};
from_091(bool, V) -> {boolean, V};
from_091(binary, V) -> {binary, V};
from_091(timestamp, V) -> {timestamp, V * 1000};
from_091(byte, V) -> {byte, V};
from_091(void, _V) -> null.

utf8(T) -> {utf8, T}.
symbol(T) -> {symbol, T}.

message_id({uuid, UUID}, HKey, H0) ->
    H = [{HKey, longstr, <<"uuid">>} | H0],
    {H, rabbit_data_coercion:to_binary(rabbit_guid:to_string(UUID))};
message_id({ulong, N}, HKey, H0) ->
    H = [{HKey, longstr, <<"ulong">>} | H0],
    {H, erlang:integer_to_binary(N)};
message_id({binary, B}, HKey, H0) ->
    E = base64:encode(B),
    case byte_size(E) > 256 of
        true ->
            K = binary:replace(HKey, <<"-type">>, <<>>),
            {[{K, longstr, B} | H0], undefined};
        false ->
            H = [{HKey, longstr, <<"binary">>} | H0],
            {H, E}
    end;
message_id({utf8, S}, HKey, H0) ->
    case byte_size(S) > 256 of
        true ->
            K = binary:replace(HKey, <<"-type">>, <<>>),
            {[{K, longstr, S} | H0], undefined};
        false ->
            {H0, S}
    end;
message_id(MsgId, _, H) ->
    {H, unwrap(MsgId)}.

unsupported_header_value_type(array) ->
    true;
unsupported_header_value_type(table) ->
    true;
unsupported_header_value_type(_) ->
    false.

filtered_header(?AMQP10_PROPERTIES_HEADER) ->
    true;
filtered_header(?AMQP10_APP_PROPERTIES_HEADER) ->
    true;
filtered_header(?AMQP10_MESSAGE_ANNOTATIONS_HEADER) ->
    true;
filtered_header(_) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
