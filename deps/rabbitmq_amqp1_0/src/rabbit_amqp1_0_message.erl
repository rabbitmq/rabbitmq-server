%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_message).

-export([assemble/1, annotated_message/3]).

-define(PROPERTIES_HEADER, <<"x-amqp-1.0-properties">>).
-define(APP_PROPERTIES_HEADER, <<"x-amqp-1.0-app-properties">>).
-define(MESSAGE_ANNOTATIONS_HEADER, <<"x-amqp-1.0-message-annotations">>).
-define(STREAM_OFFSET_HEADER, <<"x-stream-offset">>).
-define(FOOTER, <<"x-amqp-1.0-footer">>).
-define(X_DELIVERY_COUNT, <<"x-delivery-count">>).
-define(CONVERT_AMQP091_HEADERS_TO_APP_PROPS, application:get_env(rabbitmq_amqp1_0, convert_amqp091_headers_to_app_props, false)).
-define(CONVERT_APP_PROPS_TO_AMQP091_HEADERS, application:get_env(rabbitmq_amqp1_0, convert_app_props_to_amqp091_headers, false)).


-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

assemble(MsgBin) ->
    {RKey, Props, Content0} = assemble(header, {<<"">>, #'P_basic'{}, []},
                                      decode_section(MsgBin), MsgBin),

    Content1 = case Content0 of
                   Sections when is_list(Content0) ->
                       lists:reverse(Sections);
                   _ ->
                       Content0
               end,
    {RKey, #amqp_msg{props = Props, payload = Content1}}.

assemble(header, {R, P, C}, {H = #'v1_0.header'{}, Rest}, _Uneaten) ->
    assemble(message_annotations, {R, translate_header(H, P), C},
             decode_section(Rest), Rest);
assemble(header, {R, P, C}, Else, Uneaten) ->
    assemble(message_annotations, {R, P, C}, Else, Uneaten);

%% This clause doesn't get called, and is commented out as not to confuse dialyzer.
%%
%% assemble(delivery_annotations, RPC, {#'v1_0.delivery_annotations'{}, Rest},
%%          Uneaten) ->
%%     %% ignore delivery annotations for now
%%     %% TODO: handle "rejected" error
%%     assemble(message_annotations, RPC, Rest, Uneaten);
%% assemble(delivery_annotations, RPC, Else, Uneaten) ->
%%     assemble(message_annotations, RPC, Else, Uneaten);

assemble(message_annotations, {R, P = #'P_basic'{headers = Headers}, C},
         {#'v1_0.message_annotations'{}, Rest}, Uneaten) ->
    MsgAnnoBin = chunk(Rest, Uneaten),
    assemble(properties, {R, P#'P_basic'{
                               headers = set_header(?MESSAGE_ANNOTATIONS_HEADER,
                                                    MsgAnnoBin, Headers)}, C},
             decode_section(Rest), Rest);
assemble(message_annotations, {R, P, C}, Else, Uneaten) ->
    assemble(properties, {R, P, C}, Else, Uneaten);

assemble(properties, {_R, P, C}, {X = #'v1_0.properties'{}, Rest}, Uneaten) ->
    PropsBin = chunk(Rest, Uneaten),
    assemble(app_properties, {routing_key(X),
                    translate_properties(X, PropsBin, P), C},
             decode_section(Rest), Rest);
assemble(properties, {R, P, C}, Else, Uneaten) ->
    assemble(app_properties, {R, P, C}, Else, Uneaten);

assemble(app_properties, {R, P = #'P_basic'{headers = Headers}, C},
         {#'v1_0.application_properties'{}, Rest}, Uneaten) ->
    AppPropsBin = chunk(Rest, Uneaten),
    Amqp091Headers = case ?CONVERT_APP_PROPS_TO_AMQP091_HEADERS of 
                        true -> 
                            amqp10_app_props_to_amqp091_headers(Headers, AppPropsBin);
                        _ -> 
                            Headers
                     end,
    AppPropsAdded = set_header(
        ?APP_PROPERTIES_HEADER,
        AppPropsBin, Amqp091Headers),
    assemble(body, {R, P#'P_basic'{
                         headers = AppPropsAdded}, C},
             decode_section(Rest), Rest);
assemble(app_properties, {R, P, C}, Else, Uneaten) ->
    assemble(body, {R, P, C}, Else, Uneaten);

%% The only 'interoperable' content is a single amqp-data section.
%% Everything else we will leave as-is. We still have to parse the
%% sections one-by-one, however, to see when we hit the footer or
%% whatever comes next.

%% NB we do not strictly enforce the (slightly random) rules
%% pertaining to body sections, that is:
%%  - one amqp-value; OR
%%  - one or more amqp-sequence; OR
%%  - one or more amqp-data.
%% We allow any number of each kind, in any permutation.

assemble(body, {R, P, _}, {#'v1_0.data'{content = Content}, Rest}, Uneaten) ->
    Chunk = chunk(Rest, Uneaten),
    assemble(amqp10body, {R, set_1_0_type(<<"binary">>, P),
                          {data, Content, Chunk}},
             decode_section(Rest), Rest);
assemble(body, {R, P, C}, Else, Uneaten) ->
    assemble(amqp10body, {R, P, C}, Else, Uneaten);

assemble(amqp10body, {R, P, C}, {{Type, _}, Rest}, Uneaten)
  when Type =:= 'v1_0.data' orelse
       Type =:= 'v1_0.amqp_sequence' orelse
       Type =:= 'v1_0.amqp_value' ->
    Encoded = chunk(Rest, Uneaten),
    assemble(amqp10body,
             {R, set_1_0_type(<<"amqp-1.0">>, P), add_body_section(Encoded, C)},
             decode_section(Rest), Rest);
assemble(amqp10body, {R, P, C}, Else, Uneaten) ->
    assemble(footer, {R, P, compile_body(C)}, Else, Uneaten);

assemble(footer, {R, P = #'P_basic'{headers = Headers}, C},
         {#'v1_0.footer'{}, <<>>}, Uneaten) ->
    {R, P#'P_basic'{headers = set_header(?FOOTER, Uneaten, Headers)}, C};
assemble(footer, {R, P, C}, none, _) ->
    {R, P, C};
assemble(footer, _, Else, _) ->
    exit({unexpected_trailing_sections, Else}).

%% Catch-all clause, not needed according to dialyzer
%% assemble(Expected, _, Actual, _) ->
%%     exit({expected_section, Expected, Actual}).

decode_section(<<>>) ->
    none;
decode_section(MsgBin) ->
    {AmqpValue, Rest} = amqp10_binary_parser:parse(MsgBin),
    {amqp10_framing:decode(AmqpValue), Rest}.

chunk(Rest, Uneaten) ->
    ChunkLen = size(Uneaten) - size(Rest),
    <<Chunk:ChunkLen/binary, _ActuallyRest/binary>> = Uneaten,
    Chunk.

add_body_section(C, {data, _, Bin}) ->
    [C, Bin];
add_body_section(C, Cs) ->
    [C | Cs].

compile_body({data, Content, _}) ->
    Content;
compile_body(Sections) ->
    lists:reverse(Sections).

translate_header(Header10, Props) ->
    Props#'P_basic'{
      delivery_mode = case Header10#'v1_0.header'.durable of
                          true -> 2;
                          _    -> 1
                      end,
      priority = unwrap(Header10#'v1_0.header'.priority),
      expiration = to_expiration(Header10#'v1_0.header'.ttl),
      type = undefined,
      app_id = undefined,
      cluster_id = undefined}.

translate_properties(Props10, Props10Bin,
                     Props = #'P_basic'{headers = Headers}) ->
    Props#'P_basic'{
      headers          = set_header(?PROPERTIES_HEADER, Props10Bin,
                                     Headers),
      content_type     = unwrap(Props10#'v1_0.properties'.content_type),
      content_encoding = unwrap(Props10#'v1_0.properties'.content_encoding),
      correlation_id   = unwrap(Props10#'v1_0.properties'.correlation_id),
      reply_to         = case unwrap(Props10#'v1_0.properties'.reply_to) of
                             <<"/queue/", Q/binary>> -> Q;
                             Else                    -> Else
                         end,
      message_id       = unwrap(Props10#'v1_0.properties'.message_id),
      user_id          = unwrap(Props10#'v1_0.properties'.user_id),
      timestamp        = unwrap(Props10#'v1_0.properties'.creation_time)}.

routing_key(Props10) ->
    unwrap(Props10#'v1_0.properties'.subject).

unwrap(undefined)      -> undefined;
unwrap({_Type, Thing}) -> Thing.

to_expiration(undefined) ->
    undefined;
to_expiration({uint, Num}) ->
    list_to_binary(integer_to_list(Num)).

from_expiration(PBasic) ->
    case rabbit_basic:parse_expiration(PBasic) of
        {ok, undefined} -> undefined;
        {ok, N} -> {uint, N};
        _ -> undefined
    end.

set_header(Header, Value, undefined) ->
    set_header(Header, Value, []);
set_header(Header, Value, Headers) ->
    rabbit_misc:set_table_value(Headers, Header, longstr, Value).

set_1_0_type(Type, Props = #'P_basic'{}) ->
    Props#'P_basic'{type = Type}.

%%--------------------------------------------------------------------

%% TODO create delivery-annotations

annotated_message(RKey, #'basic.deliver'{redelivered = Redelivered},
                  #amqp_msg{props = Props,
                            payload = Content}) ->
    #'P_basic'{ headers = Headers } = Props,
    Header10 = #'v1_0.header'
      {durable = case Props#'P_basic'.delivery_mode of
                     2 -> true;
                     _ -> false
                 end,
       priority = wrap(ubyte, Props#'P_basic'.priority),
       ttl = from_expiration(Props),
       first_acquirer = not Redelivered,
       delivery_count = case Redelivered of
                            true -> deliverycount_from_headers(Headers);
                            false -> undefined
                        end},
    HeadersBin = amqp10_framing:encode_bin(Header10),
    MsgAnnoBin0 =
        case table_lookup(Headers, ?MESSAGE_ANNOTATIONS_HEADER) of
            undefined  -> <<>>;
            {_, MABin} -> MABin
        end,
    MsgAnnoBin =
        case table_lookup(Headers, ?STREAM_OFFSET_HEADER) of
            undefined ->
                MsgAnnoBin0;
            {_, StreamOffset} when is_integer(StreamOffset) ->
                case amqp10_framing:decode_bin(MsgAnnoBin0) of
                    [#'v1_0.message_annotations'{content = C0} = MA] ->
                        Contents = map_add(utf8, ?STREAM_OFFSET_HEADER,
                                           ulong, StreamOffset, C0),
                        amqp10_framing:encode_bin(
                          MA#'v1_0.message_annotations'{content = Contents});
                    [] ->
                        Contents = map_add(utf8, ?STREAM_OFFSET_HEADER,
                                           ulong, StreamOffset, []),
                        amqp10_framing:encode_bin(
                          #'v1_0.message_annotations'{content = Contents})
                end
        end,
    PropsBin =
        case table_lookup(Headers, ?PROPERTIES_HEADER) of
            {_, Props10Bin} ->
                Props10Bin;
            undefined ->
                Props10 = #'v1_0.properties'{
                  message_id = wrap(utf8, Props#'P_basic'.message_id),
                  user_id = wrap(utf8, Props#'P_basic'.user_id),
                  to = undefined,
                  subject = wrap(utf8, RKey),
                  reply_to = case Props#'P_basic'.reply_to of
                                 undefined ->
                                     undefined;
                                 _ ->
                                     wrap(utf8,
                                          <<"/queue/",
                                            (Props#'P_basic'.reply_to)/binary>>)
                             end,
                  correlation_id = wrap(utf8, Props#'P_basic'.correlation_id),
                  content_type = wrap(symbol, Props#'P_basic'.content_type),
                  content_encoding = wrap(symbol, Props#'P_basic'.content_encoding),
                  creation_time = wrap(timestamp, Props#'P_basic'.timestamp)},
                amqp10_framing:encode_bin(Props10)
        end,
    AppPropsBin =
        case table_lookup(Headers, ?APP_PROPERTIES_HEADER) of
            {_, AppProps10Bin} ->
                AppProps10Bin;
            undefined ->
                case ?CONVERT_AMQP091_HEADERS_TO_APP_PROPS of 
                    true -> 
                        case amqp091_headers_to_amqp10_app_props(Headers) of
                            undefined -> [];
                            Other -> 
                                amqp10_framing:encode_bin(Other)
                        end;
                    _ -> 
                        []
                end
        end,
    DataBin = case Props#'P_basic'.type of
                  <<"amqp-1.0">> ->
                      Content;
                  _Else -> % e.g., <<"binary">> if originally from 1.0
                      amqp10_framing:encode_bin(
                        #'v1_0.data'{content = Content})
              end,
    FooterBin =
        case table_lookup(Headers, ?FOOTER) of
            undefined -> <<>>;
            {_, FBin} -> FBin
    end,
    [HeadersBin, MsgAnnoBin, PropsBin, AppPropsBin, DataBin, FooterBin].

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.

table_lookup(undefined, _)    -> undefined;
table_lookup(Headers, Header) -> rabbit_misc:table_lookup(Headers, Header).

map_add(KeyType, Key, Type, Value, Acc) ->
    [{wrap(KeyType, Key), wrap(Type, Value)} | Acc].

amqp10_app_props_to_amqp091_headers(CurrentHeaders, AppPropsBin) -> 
    case amqp10_framing:decode_bin(AppPropsBin) of 
        [#'v1_0.application_properties'{ content = AppProps}] when is_list(AppProps) -> 
            Hs = case CurrentHeaders of 
                undefined -> [];
                Headers -> Headers
            end,
            lists:foldl(fun(Prop, Acc) -> 
                            case Prop of 
                                {{utf8, Key}, {ValueType, Value}} -> 
                                    case type10_to_type091(Key, ValueType, Value) of 
                                        undefined -> Acc;
                                        Typed -> [Typed |Acc]
                                    end;
                                _ -> Acc
                            end
                         end, Hs, AppProps);
        _ -> CurrentHeaders
    end.
type10_to_type091(Key, Type, Value) -> 
    try
        rabbit_msg_record:to_091(Key, {Type, Value})
    catch
        _:function_clause -> undefined
    end.

amqp091_headers_to_amqp10_app_props(undefined) -> undefined;
amqp091_headers_to_amqp10_app_props(Headers) when is_list(Headers) -> 
    AppPropsOut = lists:foldl(fun(H, Acc) -> 
        case H of 
            {Key, Type, Value} -> 
                case type091_to_type10(Type, Value) of 
                    undefined -> Acc;
                    Typed -> 
                        [{{utf8, Key}, Typed}|Acc]
                end;
            _ -> Acc
        end
    end, [], Headers),
    #'v1_0.application_properties'{content = AppPropsOut}.
    
type091_to_type10(Type, Value) ->
    try
        rabbit_msg_record:from_091(Type, Value)
    catch
        _:function_clause -> undefined
    end.

deliverycount_from_headers(Headers) -> 
    case table_lookup(Headers, ?X_DELIVERY_COUNT) of
            undefined -> undefined;
            {_, Value} when is_integer(Value) -> wrap(uint,Value);
            _ -> undefined
    end.

