%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is VMware, Inc.
%%  Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_top_util).

-export([toplist/3]).

toplist(Key, Count, List) ->
    Sorted = lists:sublist(
               lists:reverse(
                 lists:keysort(1, [toplist(Key, I) || I <- List])), Count),
    [fmt_all(Info) || {_, Info} <- Sorted].

toplist(Key, Info) ->
    {Key, Val} = lists:keyfind(Key, 1, Info),
    {Val, Info}.

fmt_all(Info) ->
    [{K, fmt(K, V)} || {K, V} <- Info].

fmt(_K, Pid) when is_pid(Pid) ->
    list_to_binary(rabbit_misc:pid_to_string(Pid));
fmt(registered_name, Name) ->
    list_to_binary(rabbit_misc:format("~s", [Name]));
fmt(_K, Other) ->
    list_to_binary(rabbit_misc:format("~p", [Other])).
