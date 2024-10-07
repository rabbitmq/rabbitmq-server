%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% AMQP Filter Expressions Version 1.0 Working Draft 09
%% https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227
-module(rabbit_amqp_filtex).

-include_lib("amqp10_common/include/amqp10_filtex.hrl").

-export([validate/1,
         filter/2]).

-type simple_type() :: number() | binary() | atom().
-type affix() :: {suffix, non_neg_integer(), binary()} |
                 {prefix, non_neg_integer(), binary()}.
-type filter_expression_value() :: simple_type() | affix().
-type filter_expression() :: {properties, [{FieldName :: atom(), filter_expression_value()}]} |
                             {application_properties, [{binary(), filter_expression_value()}]}.
-type filter_expressions() :: [filter_expression()].
-export_type([filter_expressions/0]).

-spec validate(tuple()) ->
    {ok, filter_expression()} | error.
validate({described, Descriptor, {map, KVList}}) ->
    try validate0(Descriptor, KVList)
    catch throw:{?MODULE, _, _} ->
              error
    end;
validate(_) ->
    error.

-spec filter(filter_expressions(), mc:state()) ->
    boolean().
filter(Filters, Mc) ->
    %% "A message will pass through a filter-set if and only if
    %% it passes through each of the named filters." [3.5.8]
    lists:all(fun(Filter) ->
                      filter0(Filter, Mc)
              end, Filters).

%%%%%%%%%%%%%%%%
%%% Internal %%%
%%%%%%%%%%%%%%%%

filter0({properties, KVList}, Mc) ->
    %% "The filter evaluates to true if all properties enclosed in the filter expression
    %% match the respective properties in the message."
    %% [filtex-v1.0-wd09 4.2.4]
    lists:all(fun({FieldName, RefVal}) ->
                      TaggedVal = mc:property(FieldName, Mc),
                      Val = unwrap(TaggedVal),
                      match_simple_type(RefVal, Val)
              end, KVList);
filter0({application_properties, KVList}, Mc) ->
    AppProps = mc:routing_headers(Mc, []),
    %% "The filter evaluates to true if all properties enclosed in the filter expression
    %% match the respective entries in the application-properties section in the message."
    %% [filtex-v1.0-wd09 4.2.5]
    lists:all(fun({Key, RefVal}) ->
                      case AppProps of
                          #{Key := Val} ->
                              match_simple_type(RefVal, Val);
                          _ ->
                              false
                      end
              end, KVList).

%% [filtex-v1.0-wd09 4.1.1]
%% "A reference field value in a property filter expression matches
%% its corresponding message metadata field value if:
%% [...]
match_simple_type(null, _Val) ->
    %% * The reference field value is NULL
    true;
match_simple_type({suffix, SuffixSize, Suffix}, Val) ->
    %% * Suffix. The message metadata field matches the expression if the ordinal values of the
    %% characters of the suffix expression equal the ordinal values of the same number of
    %% characters trailing the message metadata field value.
    case is_binary(Val) of
        true ->
            case Val of
                <<_:(size(Val) - SuffixSize)/binary, Suffix:SuffixSize/binary>> ->
                    true;
                _ ->
                    false
            end;
        false ->
            false
    end;
match_simple_type({prefix, PrefixSize, Prefix}, Val) ->
    %% * Prefix. The message metadata field matches the expression if the ordinal values of the
    %% characters of the prefix expression equal the ordinal values of the same number of
    %% characters leading the message metadata field value.
    case Val of
        <<Prefix:PrefixSize/binary, _/binary>> ->
            true;
        _ ->
            false
    end;
match_simple_type(RefVal, Val) ->
    %% * the reference field value is of a floating-point or integer number type
    %%   and the message metadata field is of a different floating-point or integer number type,
    %%   the reference value and the metadata field value are within the value range of both types,
    %%   and the values are equal when treated as a floating-point"
    RefVal == Val.

validate0(Descriptor, KVList) when
      (Descriptor =:= {symbol, ?DESCRIPTOR_NAME_PROPERTIES_FILTER} orelse
       Descriptor =:= {ulong, ?DESCRIPTOR_CODE_PROPERTIES_FILTER}) andalso
      KVList =/= [] ->
    validate_props(KVList, []);
validate0(Descriptor, KVList0) when
      (Descriptor =:= {symbol, ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER} orelse
       Descriptor =:= {ulong, ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES_FILTER}) andalso
      KVList0 =/= [] ->
    KVList = lists:map(fun({{utf8, Key}, {utf8, String}}) ->
                               {Key, parse_string_modifier_prefix(String)};
                          ({{utf8, Key}, TaggedVal}) ->
                               {Key, unwrap(TaggedVal)}
                       end, KVList0),
    {ok, {application_properties, KVList}};
validate0(_, _) ->
    error.

validate_props([], Acc) ->
    {ok, {properties, lists:reverse(Acc)}};
validate_props([{{symbol, <<"message-id">>}, TaggedVal} | Rest], Acc) ->
    case parse_message_id(TaggedVal) of
        {ok, Val} ->
            validate_props(Rest, [{message_id, Val} | Acc]);
        error ->
            error
    end;
validate_props([{{symbol, <<"user-id">>}, {binary, Val}} | Rest], Acc) ->
    validate_props(Rest, [{user_id, Val} | Acc]);
validate_props([{{symbol, <<"to">>}, {utf8, Val}} | Rest], Acc) ->
    validate_props(Rest, [{to, parse_string_modifier_prefix(Val)} | Acc]);
validate_props([{{symbol, <<"subject">>}, {utf8, Val}} | Rest], Acc) ->
    validate_props(Rest, [{subject, parse_string_modifier_prefix(Val)} | Acc]);
validate_props([{{symbol, <<"reply-to">>}, {utf8, Val}} | Rest], Acc) ->
    validate_props(Rest, [{reply_to, parse_string_modifier_prefix(Val)} | Acc]);
validate_props([{{symbol, <<"correlation-id">>}, TaggedVal} | Rest], Acc) ->
    case parse_message_id(TaggedVal) of
        {ok, Val} ->
            validate_props(Rest, [{correlation_id, Val} | Acc]);
        error ->
            error
    end;
validate_props([{{symbol, <<"content-type">>}, {symbol, Val}} | Rest], Acc) ->
    validate_props(Rest, [{content_type, Val} | Acc]);
validate_props([{{symbol, <<"content-encoding">>}, {symbol, Val}} | Rest], Acc) ->
    validate_props(Rest, [{content_encoding, Val} | Acc]);
validate_props([{{symbol, <<"absolute-expiry-time">>}, {timestamp, Val}} | Rest], Acc) ->
    validate_props(Rest, [{absolute_expiry_time, Val} | Acc]);
validate_props([{{symbol, <<"creation-time">>}, {timestamp, Val}} | Rest], Acc) ->
    validate_props(Rest, [{creation_time, Val} | Acc]);
validate_props([{{symbol, <<"group-id">>}, {utf8, Val}} | Rest], Acc) ->
    validate_props(Rest, [{group_id, parse_string_modifier_prefix(Val)} | Acc]);
validate_props([{{symbol, <<"group-sequence">>}, {uint, Val}} | Rest], Acc) ->
    validate_props(Rest, [{group_sequence, Val} | Acc]);
validate_props([{{symbol, <<"reply-to-group-id">>}, {utf8, Val}} | Rest], Acc) ->
    validate_props(Rest, [{reply_to_group_id, parse_string_modifier_prefix(Val)} | Acc]);
validate_props(_, _) ->
    error.

parse_message_id({ulong, Val}) ->
    {ok, Val};
parse_message_id({uuid, Val}) ->
    {ok, Val};
parse_message_id({binary, Val}) ->
    {ok, Val};
parse_message_id({utf8, Val}) ->
    {ok, parse_string_modifier_prefix(Val)};
parse_message_id(_) ->
    error.

%% [filtex-v1.0-wd09 4.1.1]
parse_string_modifier_prefix(<<"$s:", Suffix/binary>>) ->
    {suffix, size(Suffix), Suffix};
parse_string_modifier_prefix(<<"$p:", Prefix/binary>>) ->
    {prefix, size(Prefix), Prefix};
parse_string_modifier_prefix(<<"$$", _/binary>> = String) ->
    %% "Escape prefix for case-sensitive matching of a string starting with ‘&’"
    string:slice(String, 1);
parse_string_modifier_prefix(<<"$", _/binary>> = String) ->
    throw({?MODULE, invalid_reference_field_value, String});
parse_string_modifier_prefix(String) ->
    String.

unwrap({_Tag, V}) ->
    V;
unwrap(V) ->
    V.
