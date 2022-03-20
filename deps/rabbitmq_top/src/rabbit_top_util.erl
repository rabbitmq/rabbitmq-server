%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_top_util).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([toplist/3, fmt_all/1, fmt/1, obtain_name/1, safe_process_info/2]).
-export([sort_by_param/2, sort_order_param/1, row_count_param/2]).

toplist(Key, Count, List) ->
    Sorted = lists:sublist(
               lists:reverse(
                 lists:keysort(1, [toplist(Key, I) || I <- List])), Count),
    [add_name(Info) || {_, Info} <- Sorted].

toplist(Key, Info) ->
    {Key, Val} = lists:keyfind(Key, 1, Info),
    {Val, Info}.

sort_by_param(ReqData, Default) ->
    case rabbit_mgmt_util:qs_val(<<"sort">>, ReqData) of
        undefined -> Default;
        Bin       -> rabbit_data_coercion:to_atom(Bin)
    end.

sort_order_param(ReqData) ->
    case rabbit_mgmt_util:qs_val(<<"sort_reverse">>, ReqData) of
        <<"true">> -> asc;
        _          -> desc
    end.

row_count_param(ReqData, Default) ->
    case rabbit_mgmt_util:qs_val(<<"row_count">>, ReqData) of
        undefined -> Default;
        Bin       -> rabbit_data_coercion:to_integer(Bin)
    end.

add_name(Info) ->
    {pid, Pid} = lists:keyfind(pid, 1, Info),
    [{name, obtain_name(Pid)} | Info].

fmt_all(Info) -> [{K, fmt(V)} || {K, V} <- Info].

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
    case safe_process_info(Pid, registered_name) of
        {registered_name, Name} -> [{type, registered},
                                    {name, Name}];
        _                       -> fail
    end.

obtain_from_process_name(Pid) ->
    case safe_process_info(Pid, dictionary) of
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
     {connection_name, ConnName}];

fmt_process_name({Type, unknown}) -> %% probably some adapter thing
    [{supertype,       connection},
     {type,            Type},
     {connection_name, unknown}].

obtain_from_initial_call(Pid) ->
    case initial_call(Pid) of
        fail -> [{type, starting},
                 {name, fmt(Pid)}];
        MFA  -> case guess_initial_call(MFA) of
                    fail -> [{type, unknown},
                             {name, fmt(MFA)}];
                    Name -> [{type, known},
                             {name, Name}]
                end
    end.

initial_call(Pid) ->
    case initial_call_dict(Pid) of
        fail -> case safe_process_info(Pid, initial_call) of
                    {initial_call, MFA} -> MFA;
                    _                   -> fail
                end;
        MFA  -> MFA
    end.

initial_call_dict(Pid) ->
    case safe_process_info(Pid, dictionary) of
        {dictionary, Dict} ->
            case lists:keyfind('$initial_call', 1, Dict) of
                {'$initial_call', MFA} -> MFA;
                false                  -> fail
            end;
        _ ->
            fail
    end.

guess_initial_call({supervisor, _F, _A})      -> supervisor;
guess_initial_call({supervisor2, _F, _A})     -> supervisor;
guess_initial_call({cowboy_protocol, _F, _A}) -> cowboy_protocol;
guess_initial_call(_MFA)                      -> fail.


safe_process_info(Pid, Info) ->
    rpc:call(node(Pid), erlang, process_info, [Pid, Info]).
