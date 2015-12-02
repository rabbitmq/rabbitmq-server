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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%


-module(rabbit_resource_monitor_misc).

-export([parse_information_unit/1]).

-ifdef(use_spec).

-spec(parse_information_unit/1 :: (integer() | string()) -> 
    {ok, integer()} | {error, parse_error}).

-endif.

parse_information_unit(MemLim) when is_integer(MemLim) -> {ok, MemLim};
parse_information_unit(MemLim) when is_list(MemLim) ->
    case re:run(MemLim, 
                "^(?<VAL>[0-9]+)(?<UNIT>kB|MB|GB|kiB|MiB|GiB|k|M|G)?$", 
                [{capture, all_names, list}]) of
    	{match, [[], _]} ->
        	{ok, list_to_integer(MemLim)};    		
        {match, [Unit, Num]} ->
            Multiplier = case Unit of
                KiB when KiB == "k"; KiB == "kiB" -> 1024;
                MiB when MiB == "M"; MiB == "MiB" -> 1024*1024;
                GiB when GiB == "G"; GiB == "GiB" -> 1024*1024*1024;
                "KB" -> 1000;
                "MB" -> 1000000;
                "GB" -> 1000000000
            end,
            {ok, Num * Multiplier};
        nomatch ->
            % log error
            {error, parse_error}
    end.