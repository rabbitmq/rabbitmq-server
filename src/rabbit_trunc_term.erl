%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_trunc_term).

-export([truncate_log_event/1]).
%% exported for testing
-export([shrink_term/1, shrink_term/2]).

truncate_log_event({Type, GL, {Pid, Format, Args}})
  when Type =:= error orelse
       Type =:= info_msg orelse
       Type =:= warning_msg ->
    {Type, GL, {Pid, Format, [shrink_term(T) || T <- Args]}};
truncate_log_event({Type, GL, {Pid, ReportType, Report}})
  when Type =:= error_report orelse
       Type =:= info_report orelse
       Type =:= warning_report ->
    Report2 = case ReportType of
                  crash_report -> [[{K, shrink_term(V)} || {K, V} <- R] ||
                                      R <- Report];
                  _            -> [{K, shrink_term(V)} || {K, V} <- Report]
              end,
    {Type, GL, {Pid, ReportType, Report2}};
truncate_log_event(Event) ->
    Event.

shrink_term(T) -> shrink_term(T, 10).

%% TODO: avoid copying
%% TODO: can we get away with using binary:part/3 (OTP vsn requirements)?
%% TODO: reconsider depth limit handling
shrink_term(T, 0) -> T;
shrink_term(T, N) when is_binary(T) andalso size(T) > N ->
    L = N - 3,
    case size(T) - L of
        Sz when Sz >= 1 -> <<Head:L/binary, _/binary>> = T,
                           <<Head/binary, <<"...">>/binary>>;
        _               -> T
    end;
shrink_term([A|B], N) when not is_list(B) ->
    shrink_term([A,B], N);
shrink_term(T, N) when is_list(T) ->
    IsPrintable = io_lib:printable_list(T),
    case length(T) > N of
        true when IsPrintable ->
            lists:append(lists:sublist(T, suffix_len(N-3)), "...");
        true ->
            lists:append([shrink_term(E, N-1) ||
                             E <- lists:sublist(T, suffix_len(N-1))],
                         ['...']);
        false when IsPrintable ->
            T;
        false ->
            [shrink_term(E, N-1) || E <- T]
    end;
shrink_term(T, N) when is_tuple(T) ->
    case tuple_size(T) > N of
        true  -> list_to_tuple(
                   lists:append([shrink_term(E, N-1) ||
                                    E <- lists:sublist(tuple_to_list(T), N-1)],
                                ['...']));
        false -> list_to_tuple([shrink_term(E, N-1) ||
                                   E <- tuple_to_list(T)])
    end;
shrink_term(T, _) -> T.

suffix_len(N) -> erlang:max(N, 1).

