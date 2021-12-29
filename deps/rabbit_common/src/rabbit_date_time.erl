%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_date_time).

-export([parse_duration/1]).

-type datetime_plist() :: list({atom(), integer()}).

% from https://github.com/erlsci/iso8601/blob/main/src/iso8601.erl
-spec gi(string()) -> integer().
gi(DS) ->
    {Int, _Rest} = string:to_integer(DS),
    case Int of
        error ->
            0;
        _ ->
            Int
    end.

-spec parse_duration(string()) -> datetime_plist().
parse_duration(Bin)
    when is_binary(Bin) -> %TODO extended format
    parse_duration(binary_to_list(Bin));
parse_duration(Str) ->
    case re:run(Str,
                "^(?<sign>-|\\+)?P(?:(?<years>[0-9]+)Y)?(?:(?<months>[0"
                "-9]+)M)?(?:(?<days>[0-9]+)D)?(T(?:(?<hours>[0-9]+)H)?("
                "?:(?<minutes>[0-9]+)M)?(?:(?<seconds>[0-9]+(?:\\.[0-9]"
                "+)?)S)?)?$",
                [{capture, [sign, years, months, days, hours, minutes, seconds],
                  list}])
    of
        {match, [Sign, Years, Months, Days, Hours, Minutes, Seconds]} ->
            {ok, [{sign, Sign},
                  {years, gi(Years)},
                  {months, gi(Months)},
                  {days, gi(Days)},
                  {hours, gi(Hours)},
                  {minutes, gi(Minutes)},
                  {seconds, gi(Seconds)}]};
        nomatch ->
            error
    end.
