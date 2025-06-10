%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% AMQP Filter Expressions Version 1.0 Working Draft 09
%% §4: Property Filter Expressions
%% https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227
-module(rabbit_amqp_filter_prop).

-include_lib("amqp10_common/include/amqp10_filter.hrl").

-export([parse/1,
         eval/2]).

%% "Impose a limit on the complexity of each filter expression."
%% [filtex-v1.0-wd09 7.1]
-define(MAX_FILTER_FIELDS, 16).

-type simple_type() :: number() | binary() | atom().
-type affix() :: {suffix, non_neg_integer(), binary()} |
                 {prefix, non_neg_integer(), binary()}.
-type parsed_expression_value() :: simple_type() | affix().
-type parsed_expression() :: {properties, [{FieldName :: atom(), parsed_expression_value()}, ...]} |
                             {application_properties, [{binary(), parsed_expression_value()}, ...]}.
-type parsed_expressions() :: [parsed_expression()].
-export_type([parsed_expressions/0]).

-spec parse(tuple()) ->
    {ok, parsed_expression()} | error.
parse({described, Descriptor, {map, KVList}})
  when KVList =/= [] andalso
       length(KVList) =< ?MAX_FILTER_FIELDS ->
    try parse0(Descriptor, KVList)
    catch throw:{?MODULE, _, _} ->
              error
    end;
parse(_) ->
    error.

-spec eval(parsed_expressions(), mc:state()) ->
    boolean().
eval(Filters, Mc) ->
    %% "A message will pass through a filter-set if and only if
    %% it passes through each of the named filters." [3.5.8]
    lists:all(fun(Filter) ->
                      filter(Filter, Mc)
              end, Filters).

%%%%%%%%%%%%%%%%
%%% Internal %%%
%%%%%%%%%%%%%%%%

filter({properties, KVList}, Mc) ->
    %% "The filter evaluates to true if all properties enclosed in the filter expression
    %% match the respective properties in the message."
    %% [filtex-v1.0-wd09 4.2.4]
    lists:all(fun({FieldName, RefVal}) ->
                      TaggedVal = mc:property(FieldName, Mc),
                      Val = unwrap(TaggedVal),
                      match_simple_type(RefVal, Val)
              end, KVList);
filter({application_properties, KVList}, Mc) ->
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
                <<_:(byte_size(Val) - SuffixSize)/binary, Suffix:SuffixSize/binary>> ->
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

parse0(Descriptor, KVList) when
      Descriptor =:= {symbol, ?DESCRIPTOR_NAME_PROPERTIES_FILTER} orelse
      Descriptor =:= {ulong, ?DESCRIPTOR_CODE_PROPERTIES_FILTER} ->
    parse_props(KVList, []);
parse0(Descriptor, KVList) when
      Descriptor =:= {symbol, ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER} orelse
      Descriptor =:= {ulong, ?DESCRIPTOR_CODE_APPLICATION_PROPERTIES_FILTER} ->
    parse_app_props(KVList, []);
parse0(_, _) ->
    error.

parse_props([], Acc) ->
    {ok, {properties, lists:reverse(Acc)}};
parse_props([{{symbol, <<"message-id">>}, TaggedVal} | Rest], Acc) ->
    case parse_message_id(TaggedVal) of
        {ok, Val} ->
            parse_props(Rest, [{message_id, Val} | Acc]);
        error ->
            error
    end;
parse_props([{{symbol, <<"user-id">>}, {binary, Val}} | Rest], Acc) ->
    parse_props(Rest, [{user_id, Val} | Acc]);
parse_props([{{symbol, <<"to">>}, {utf8, Val}} | Rest], Acc) ->
    parse_props(Rest, [{to, parse_string_modifier_prefix(Val)} | Acc]);
parse_props([{{symbol, <<"subject">>}, {utf8, Val}} | Rest], Acc) ->
    parse_props(Rest, [{subject, parse_string_modifier_prefix(Val)} | Acc]);
parse_props([{{symbol, <<"reply-to">>}, {utf8, Val}} | Rest], Acc) ->
    parse_props(Rest, [{reply_to, parse_string_modifier_prefix(Val)} | Acc]);
parse_props([{{symbol, <<"correlation-id">>}, TaggedVal} | Rest], Acc) ->
    case parse_message_id(TaggedVal) of
        {ok, Val} ->
            parse_props(Rest, [{correlation_id, Val} | Acc]);
        error ->
            error
    end;
parse_props([{{symbol, <<"content-type">>}, {symbol, Val}} | Rest], Acc) ->
    parse_props(Rest, [{content_type, Val} | Acc]);
parse_props([{{symbol, <<"content-encoding">>}, {symbol, Val}} | Rest], Acc) ->
    parse_props(Rest, [{content_encoding, Val} | Acc]);
parse_props([{{symbol, <<"absolute-expiry-time">>}, {timestamp, Val}} | Rest], Acc) ->
    parse_props(Rest, [{absolute_expiry_time, Val} | Acc]);
parse_props([{{symbol, <<"creation-time">>}, {timestamp, Val}} | Rest], Acc) ->
    parse_props(Rest, [{creation_time, Val} | Acc]);
parse_props([{{symbol, <<"group-id">>}, {utf8, Val}} | Rest], Acc) ->
    parse_props(Rest, [{group_id, parse_string_modifier_prefix(Val)} | Acc]);
parse_props([{{symbol, <<"group-sequence">>}, {uint, Val}} | Rest], Acc) ->
    parse_props(Rest, [{group_sequence, Val} | Acc]);
parse_props([{{symbol, <<"reply-to-group-id">>}, {utf8, Val}} | Rest], Acc) ->
    parse_props(Rest, [{reply_to_group_id, parse_string_modifier_prefix(Val)} | Acc]);
parse_props(_, _) ->
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

parse_app_props([], Acc) ->
    {ok, {application_properties, lists:reverse(Acc)}};
parse_app_props([{{utf8, Key}, {utf8, String}} | Rest], Acc) ->
    parse_app_props(Rest, [{Key, parse_string_modifier_prefix(String)} | Acc]);
parse_app_props([{{utf8, Key}, TaggedVal} | Rest], Acc) ->
    parse_app_props(Rest, [{Key, unwrap(TaggedVal)} | Acc]);
parse_app_props(_, _) ->
    error.

%% [filtex-v1.0-wd09 4.1.1]
parse_string_modifier_prefix(<<"&s:", Suffix/binary>>) ->
    {suffix, byte_size(Suffix), Suffix};
parse_string_modifier_prefix(<<"&p:", Prefix/binary>>) ->
    {prefix, byte_size(Prefix), Prefix};
parse_string_modifier_prefix(<<"&&", _/binary>> = String) ->
    %% "Escape prefix for case-sensitive matching of a string starting with ‘&’"
    string:slice(String, 1);
parse_string_modifier_prefix(<<"&", _/binary>> = String) ->
    throw({?MODULE, invalid_reference_field_value, String});
parse_string_modifier_prefix(String) ->
    String.

unwrap({_Tag, V}) ->
    V;
unwrap(V) ->
    V.
