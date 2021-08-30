%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_util).

-include("rabbit_amqp1_0.hrl").

-export([protocol_error/3]).
-export([serial_add/2, serial_compare/2, serial_diff/2]).

-export_type([serial_number/0]).
-type serial_number() :: non_neg_integer().
-type serial_compare_result() :: 'equal' | 'less' | 'greater'.

-spec serial_add(serial_number(), non_neg_integer()) ->
             serial_number().
-spec serial_compare(serial_number(), serial_number()) ->
             serial_compare_result().
-spec serial_diff(serial_number(), serial_number()) ->
             integer().

protocol_error(Condition, Msg, Args) ->
    exit(#'v1_0.error'{
        condition   = Condition,
        description = {utf8, list_to_binary(
                               lists:flatten(io_lib:format(Msg, Args)))}
       }).

%% Serial arithmetic for unsigned ints.
%% http://www.faqs.org/rfcs/rfc1982.html
%% SERIAL_BITS = 32

%% 2 ^ SERIAL_BITS
-define(SERIAL_MAX, 16#100000000).
%% 2 ^ (SERIAL_BITS - 1) - 1
-define(SERIAL_MAX_ADDEND, 16#7fffffff).

serial_add(S, N) when N =< ?SERIAL_MAX_ADDEND ->
    (S + N) rem ?SERIAL_MAX;
serial_add(S, N) ->
    exit({out_of_bound_serial_addition, S, N}).

serial_compare(A, B) ->
    if A =:= B ->
            equal;
       (A < B andalso B - A < ?SERIAL_MAX_ADDEND) orelse
       (A > B andalso A - B > ?SERIAL_MAX_ADDEND) ->
            less;
       (A < B andalso B - A > ?SERIAL_MAX_ADDEND) orelse
       (A > B andalso B - A < ?SERIAL_MAX_ADDEND) ->
            greater;
       true -> exit({indeterminate_serial_comparison, A, B})
    end.

-define(SERIAL_DIFF_BOUND, 16#80000000).

serial_diff(A, B) ->
    Diff = A - B,
    if Diff > (?SERIAL_DIFF_BOUND) ->
            %% B is actually greater than A
            - (?SERIAL_MAX - Diff);
       Diff < - (?SERIAL_DIFF_BOUND) ->
            ?SERIAL_MAX + Diff;
       Diff < ?SERIAL_DIFF_BOUND andalso Diff > -?SERIAL_DIFF_BOUND ->
            Diff;
       true ->
            exit({indeterminate_serial_diff, A, B})
    end.
