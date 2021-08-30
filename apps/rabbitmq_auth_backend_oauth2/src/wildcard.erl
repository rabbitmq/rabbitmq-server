%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
        rabbit_log:warning("Invalid pattern ~p : ~p",
                           [Pattern, {Type, Error}]),
        invalid
    end.
