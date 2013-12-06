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

-include_lib("rabbit_common/include/rabbit.hrl").

-export([toplist/3]).

toplist(Key, Count, List) ->
    Sorted = lists:sublist(
               lists:reverse(
                 lists:keysort(1, [toplist(Key, I) || I <- List])), Count),
    [fmt_all(Info) || {_, Info} <- Sorted].

toplist(Key, Info) ->
    {Key, Val} = lists:keyfind(Key, 1, Info),
    {Val, Info}.

fmt_all(Info0) ->
    {pid, Pid} = lists:keyfind(pid, 1, Info0),
    Info = [{K, fmt(K, V)} || {K, V} <- Info0],
    case process_info(Pid, dictionary) of
        {dictionary, Dict} ->
            case lists:keyfind(rabbit_process_name, 1, Dict) of
                {rabbit_process_name = K, Name} -> [{K, fmt_name(Name)} | Info];
                false                           -> Info
            end;
        undefined ->
            Info
    end.

fmt(_K, Pid) when is_pid(Pid) ->
    list_to_binary(rabbit_misc:pid_to_string(Pid));
fmt(registered_name, Name) ->
    list_to_binary(rabbit_misc:format("~s", [Name]));
fmt(_K, Other) ->
    list_to_binary(rabbit_misc:format("~p", [Other])).

fmt_name({Type, {ConnName, ChNum}}) when Type =:= channel; Type =:= writer ->
    [{type,            Type},
     {connection_name, ConnName},
     {channel_number,  ChNum}];

fmt_name({Type, #resource{virtual_host = VHost,
                          name         = Name}}) when Type =:= queue;
                                                      Type =:= queue_slave ->
    [{type,       Type},
     {queue_name, Name},
     {vhost,      VHost}];

fmt_name({reader, ConnName}) ->
    [{type,            reader},
     {connection_name, ConnName}].
