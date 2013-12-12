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

-export([toplist/3, obtain_name/1, fmt/1]).

toplist(Key, Count, List) ->
    Sorted = lists:sublist(
               lists:reverse(
                 lists:keysort(1, [toplist(Key, I) || I <- List])), Count),
    [fmt_all(Info) || {_, Info} <- Sorted].

toplist(Key, Info) ->
    {Key, Val} = lists:keyfind(Key, 1, Info),
    {Val, Info}.

fmt_all(Info) ->
    {pid, Pid} = lists:keyfind(pid, 1, Info),
    [{name, obtain_name(Pid)} | [{K, fmt(V)} || {K, V} <- Info]].

fmt(Pid) when is_pid(Pid) ->
    list_to_binary(pid_to_list(Pid));
fmt(Other) ->
    list_to_binary(rabbit_misc:format("~p", [Other])).

obtain_name(Pid) ->
    lists:foldl(fun(Fun,  fail) -> Fun(Pid);
                   (_Fun, Res)  -> Res
                end, fail, [fun obtain_from_registered_name/1,
                            fun obtain_from_process_name/1,
                            fun obtain_from_initial_call/1]).

obtain_from_registered_name(Pid) ->
    case process_info(Pid, registered_name) of
        {registered_name, Name} -> [{type, registered},
                                    {name, Name}];
        _                       -> fail
    end.

obtain_from_process_name(Pid) ->
    case process_info(Pid, dictionary) of
        {dictionary, Dict} ->
            case lists:keyfind(process_name, 1, Dict) of
                {process_name, Name} -> fmt_process_name(Name);
                false                -> fail
            end;
        _ ->
            fail
    end.

fmt_process_name({Type, {ConnName, ChNum}}) when is_binary(ConnName),
                                                 is_integer(ChNum) ->
    [{supertype,       channel},
     {type,            Type},
     {connection_name, ConnName},
     {channel_number,  ChNum}];

fmt_process_name({Type, #resource{virtual_host = VHost,
                                  name         = Name}}) ->
    [{supertype,  queue},
     {type,       Type},
     {queue_name, Name},
     {vhost,      VHost}];

fmt_process_name({Type, ConnName}) when is_binary(ConnName) ->
    [{supertype,       connection},
     {type,            Type},
     {connection_name, ConnName}].

obtain_from_initial_call(Pid) ->
    case initial_call(Pid) of
        fail -> [{type, unidentified},
                 {name, fmt(Pid)}];
        MFA  -> case guess_initial_call(MFA) of
                    fail -> [{type, guessed},
                             {name, fmt(MFA)}];
                    Name -> [{type, known},
                             {name, Name}]
                end
    end.

initial_call(Pid) ->
    case initial_call_dict(Pid) of
        fail -> case process_info(Pid, initial_call) of
                    {initial_call, MFA} -> MFA;
                    _                   -> fail
                end;
        MFA  -> MFA
    end.

initial_call_dict(Pid) ->
    case process_info(Pid, dictionary) of
        {dictionary, Dict} ->
            case lists:keyfind('$initial_call', 1, Dict) of
                {'$initial_call', MFA} -> MFA;
                false                  -> fail
            end;
        _ ->
            fail
    end.

guess_initial_call({mochiweb_acceptor, _F, _A}) -> mochiweb_http;
guess_initial_call(MFA)                         -> fail.
