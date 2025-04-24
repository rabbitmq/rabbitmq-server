%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% Evaluates AMQP property filter expressions.
%%
%% AMQP Filter Expressions Version 1.0 Working Draft 09
%% https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=66227
%%
%% § 4 Property Filter Expressions
%% § 4.2.1 header filter
%% § 4.2.4 properties filter
%% § 4.2.5 applications-properties filter
%%
%% Evaluation is very similar to filtering in rabbit_amqp_filtex,
%% but is copied here to run in the quorum queue state machine.
-module(rabbit_fifo_filter_prop).

-export([eval/2]).

-spec eval(rabbit_amqp_filter:filter_expressions(),
           #{atom() | binary() => atom() | binary() | number()}) ->
    boolean().
eval(Filters, Headers) ->
    %% "A message will pass through a filter-set if and only if
    %% it passes through each of the named filters." [3.5.8]
    lists:all(fun(Filter) ->
                      filter(Filter, Headers)
              end, Filters).

filter({properties, KVList}, Headers) ->
    %% "The filter evaluates to true if all properties enclosed in the filter expression
    %% match the respective properties in the message."
    %% [filtex-v1.0-wd09 4.2.4]
    lists:all(fun({FieldName, RefVal}) ->
                      case Headers of
                          #{FieldName := Val} ->
                              match_simple_type(RefVal, Val);
                          _ ->
                              false
                      end
              end, KVList);
filter({application_properties, KVList}, Headers) ->
    %% "The filter evaluates to true if all properties enclosed in the filter expression
    %% match the respective entries in the application-properties section in the message."
    %% [filtex-v1.0-wd09 4.2.5]
    lists:all(fun({Key, RefVal}) ->
                      case Headers of
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
