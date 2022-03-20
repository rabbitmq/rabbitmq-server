%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%


-module(rabbit_resource_monitor_misc).

-export([parse_information_unit/1]).

-spec parse_information_unit(integer() | string()) ->
          {ok, integer()} | {error, parse_error}.

parse_information_unit(Value) when is_integer(Value) -> {ok, Value};
parse_information_unit(Value0) ->
    Value = rabbit_data_coercion:to_list(Value0),
    case re:run(Value,
                "^(?<VAL>[0-9]+)(?<UNIT>kB|KB|MB|GB|kb|mb|gb|Kb|Mb|Gb|kiB|KiB|MiB|GiB|kib|mib|gib|KIB|MIB|GIB|k|K|m|M|g|G)?$",
                [{capture, all_but_first, list}]) of
    	{match, [[], _]} ->
            {ok, list_to_integer(Value)};
        {match, [Num]} ->
            {ok, list_to_integer(Num)};
        {match, [Num, Unit]} ->
            Multiplier = case Unit of
                             KiB when KiB =:= "k";  KiB =:= "kiB"; KiB =:= "K"; KiB =:= "KIB"; KiB =:= "kib" -> 1024;
                             MiB when MiB =:= "m";  MiB =:= "MiB"; MiB =:= "M"; MiB =:= "MIB"; MiB =:= "mib" -> 1024*1024;
                             GiB when GiB =:= "g";  GiB =:= "GiB"; GiB =:= "G"; GiB =:= "GIB"; GiB =:= "gib" -> 1024*1024*1024;
                             KB  when KB  =:= "KB"; KB  =:= "kB"; KB =:= "kb"; KB =:= "Kb"  -> 1000;
                             MB  when MB  =:= "MB"; MB  =:= "mB"; MB =:= "mb"; MB =:= "Mb"  -> 1000000;
                             GB  when GB  =:= "GB"; GB  =:= "gB"; GB =:= "gb"; GB =:= "Gb"  -> 1000000000
                         end,
            {ok, list_to_integer(Num) * Multiplier};
        nomatch ->
                                                % log error
            {error, parse_error}
    end.
