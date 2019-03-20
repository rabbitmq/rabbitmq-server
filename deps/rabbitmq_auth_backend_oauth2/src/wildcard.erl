%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(wildcard).

-export([match/2]).

-spec match(Subject :: binary(), Pattern :: binary()) -> boolean().
match(Subject, Pattern) ->
    case parse_pattern(Pattern) of
        [First | Rest] ->
            FirstSize = byte_size(First),
            case Subject of
                % If a pattern does not start with a wildcard,
                % do exact matching in the beginning of the subject
                <<First:FirstSize/binary, _/binary>> ->
                    scan(Subject, Rest, FirstSize, byte_size(Subject));
                _ -> false
            end;
        invalid -> false
    end.

-spec scan(Subject :: binary(), Pattern :: [binary()],
           Pos :: integer(), Length :: integer()) -> boolean().
% Pattern ends with a wildcard
scan(_Subject, [<<>>], _Pos, _Length) -> true;
% Pattern is complete. Subject scan is complete
scan(_Subject, [], Length, Length) -> true;
% No more pattern but subject scan is not complete
scan(_Subject, [], Pos, Length) when Pos =/= Length -> false;
% Subject scan is complete but there are more pattern elements
scan(_Subject, _NonEmpty, Length, Length) -> false;
% Skip duplicate wildcards
scan(Subject, [<<>> | Rest], Pos, Length) ->
    scan(Subject, Rest, Pos, Length);
% Every other Part is after a wildcard
scan(Subject, [Part | Rest], Pos, Length) ->
    PartSize = byte_size(Part),
    case binary:match(Subject, Part, [{scope, {Pos, Length - Pos}}]) of
        nomatch             -> false;
        {PartPos, PartSize} ->
            NewPos = PartPos + PartSize,
            scan(Subject, Rest, NewPos, Length)
    end.

-spec parse_pattern(binary()) -> [binary()] | invalid.
parse_pattern(Pattern) ->
    Parts = binary:split(Pattern, <<"*">>, [global]),
    try lists:map(fun(Part) -> cow_qs:urldecode(Part) end, Parts)
    catch Type:Error ->
        rabbit_log:warning("Invalid pattern ~p : ~p~n",
                           [Pattern, {Type, Error}]),
        invalid
    end.
