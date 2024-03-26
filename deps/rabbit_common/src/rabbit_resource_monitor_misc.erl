%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%


-module(rabbit_resource_monitor_misc).

-export([parse_information_unit/1]).

-spec parse_information_unit(integer() | string()) ->
          {ok, integer()} | {error, parse_error}.

parse_information_unit(Value) when is_integer(Value) -> {ok, Value};
parse_information_unit(Value0) ->
    Value = rabbit_data_coercion:to_list(Value0),
    case re:run(Value,
                "^(?<VAL>[0-9]+)(?<UNIT>kB|Ki|Mi|Gi|Ti|Pi|KB|MB|GB|TB|PB|kb|ki|mb|gb|tb|pb|Kb|Mb|Gb|Tb|Pb|kiB|KiB|MiB|GiB|TiB|PiB|kib|mib|gib|tib|pib|KIB|MIB|GIB|TIB|PIB|k|K|m|M|g|G|p|P)?$",
                [{capture, all_but_first, list}]) of
    	{match, [[], _]} ->
            {ok, list_to_integer(Value)};
        {match, [Num]} ->
            {ok, list_to_integer(Num)};
        {match, [Num, Unit]} ->
            %% Note: there is no industry standard on what K, M, G, T, P means (is G a gigabyte or a gibibyte?), so
            %% starting with 3.13 we treat those the same way as Kubernetes does [1].
            %%
            %% 1. https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
            Multiplier = case Unit of
                             KiB when KiB =:= "kiB"; KiB =:= "KIB"; KiB =:= "kib"; KiB =:= "ki"; KiB =:= "Ki" -> 1024;
                             MiB when MiB =:= "MiB"; MiB =:= "MIB"; MiB =:= "mib"; MiB =:= "mi"; MiB =:= "Mi" -> 1024 * 1024;
                             GiB when GiB =:= "GiB"; GiB =:= "GIB"; GiB =:= "gib"; GiB =:= "gi"; GiB =:= "Gi" -> 1024 * 1024 * 1024;
                             TiB when TiB =:= "TiB"; TiB =:= "TIB"; TiB =:= "tib"; TiB =:= "ti"; TiB =:= "Ti" -> 1024 * 1024 * 1024 * 1024;
                             PiB when PiB =:= "PiB"; PiB =:= "PIB"; PiB =:= "pib"; PiB =:= "pi"; PiB =:= "Pi" -> 1024 * 1024 * 1024 * 1024 * 2014;

                             KB  when KB =:= "k"; KB =:= "K"; KB  =:= "KB"; KB  =:= "kB"; KB =:= "kb"; KB =:= "Kb"  -> 1000;
                             MB  when MB =:= "m"; MB =:= "M"; MB  =:= "MB"; MB  =:= "mB"; MB =:= "mb"; MB =:= "Mb"  -> 1000_000;
                             GB  when GB =:= "g"; GB =:= "G"; GB  =:= "GB"; GB  =:= "gB"; GB =:= "gb"; GB =:= "Gb"  -> 1000_000_000;
                             TB  when TB =:= "t"; TB =:= "T"; TB  =:= "TB"; TB  =:= "tB"; TB =:= "tb"; TB =:= "Tb"  -> 1000_000_000_000;
                             PB  when PB =:= "p"; PB =:= "P"; PB  =:= "PB"; PB  =:= "pB"; PB =:= "pb"; PB =:= "Pb"  -> 1000_000_000_000_000
                         end,
            {ok, list_to_integer(Num) * Multiplier};
        nomatch ->
                                                % log error
            {error, parse_error}
    end.
