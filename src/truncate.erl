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

-module(truncate).

-export([log_event/1]).
%% exported for testing
-export([term/1, term/2]).

log_event({Type, GL, {Pid, Format, Args}})
  when Type =:= error orelse
       Type =:= info_msg orelse
       Type =:= warning_msg ->
    {Type, GL, {Pid, Format, [term(T) || T <- Args]}};
log_event({Type, GL, {Pid, ReportType, Report}})
  when Type =:= error_report orelse
       Type =:= info_report orelse
       Type =:= warning_report ->
    Report2 = case ReportType of
                  crash_report -> [[{K, term(V)} || {K, V} <- R] ||
                                      R <- Report];
                  _            -> [{K, term(V)} || {K, V} <- Report]
              end,
    {Type, GL, {Pid, ReportType, Report2}};
log_event(Event) ->
    Event.

term(T) -> term(T, 10).

%% TODO: avoid copying
%% TODO: can we get away with using binary:part/3 (OTP vsn requirements)?
%% TODO: reconsider depth limit handling
term(T, 0) -> T;
term(T, N) when is_binary(T) andalso size(T) > N ->
    Suffix = N - 3,
    Len = case is_bitstring(T) of
              true  -> byte_size(T);
              false -> size(T)
          end,
    case Len - Suffix of
        Sz when Sz >= 1 -> <<Head:Suffix/binary, _/binary>> = T,
                           <<Head/binary, <<"...">>/binary>>;
        _               -> T
    end;
term([A|B], N) when not is_list(B) ->
    term([A,B], N);
term(T, N) when is_list(T) ->
    IsPrintable = io_lib:printable_list(T),
    Len = length(T),
    case {Len > N, IsPrintable} of
        {true, true}
          when N > 3   -> lists:append(lists:sublist(T, resize(N-3)), "...");
        {true, false}  -> lists:append([term(E, resize(N-1, Len)) ||
                                           E <- lists:sublist(T, resize(N-1))],
                                       ['...']);
        {false, false} -> [term(E, resize(N-1, Len)) || E <- T];
        _              -> T
    end;
term(T, N) when is_tuple(T) ->
    case tuple_size(T) > N of
        true  -> list_to_tuple(
                   lists:append([term(E, N-1) ||
                                    E <- lists:sublist(tuple_to_list(T), N-1)],
                                ['...']));
        false -> list_to_tuple([term(E, N-1) ||
                                   E <- tuple_to_list(T)])
    end;
term(T, _) -> T.

resize(N) -> resize(N, 1).

resize(N, M) -> erlang:max(N, M).

