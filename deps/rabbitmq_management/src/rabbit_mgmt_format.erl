%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_format).

-export([encode/1, format/2, print/2, pid/1, ip/1, table/1, tuple/1]).
-export([protocol/1, resource/1, permissions/1, user_permissions/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

encode(Facts) ->
    mochijson2:encode({struct,
                       [{node, node()},
                        {datetime, list_to_binary(
                                     rabbit_mgmt_util:http_date())}
                       ] ++ Facts}).

format([], _Fs) ->
    [];
format([{_Name, unknown}|Stats], Fs) ->
    format(Stats, Fs);
format([Stat|Stats], Fs) ->
    format_item(Stat, Fs) ++ format(Stats, Fs).

format_item(Stat, []) ->
    [Stat];
format_item({Name, Value}, [{Fun, Names}|Fs]) ->
    case lists:member(Name, Names) of
        true ->
            case Fun(Value) of
                List when is_list(List) -> List;
                Formatted               -> [{Name, Formatted}]
            end;
        _    ->
            format_item({Name, Value}, Fs)
    end.

print(Fmt, Val) when is_list(Val) ->
    list_to_binary(lists:flatten(io_lib:format(Fmt, Val)));
print(Fmt, Val) ->
    print(Fmt, [Val]).

pid(Pid) when is_pid(Pid) ->
    list_to_binary(io_lib:format("~w", [Pid]));
pid('') ->
    <<"">>;
pid(unknown) ->
    unknown.

ip(unknown) ->
    unknown;
ip(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

table(unknown) ->
    unknown;
table(Table) ->
    {struct, [{Name, tuple(Value)} || {Name, _Type, Value} <- Table]}.

tuple(unknown) ->
    unknown;
tuple(Tuple) when is_tuple(Tuple) ->
    [tuple(E) || E <- tuple_to_list(Tuple)];
tuple(Term) ->
    Term.

protocol(unknown) ->
    unknown;
protocol({Major, Minor, 0}) ->
    print("~p-~p", [Major, Minor]);
protocol({Major, Minor, Revision}) ->
    print("~p-~p-~p", [Major, Minor, Revision]).

resource(unknown) ->
    unknown;
resource(#resource{name = Name, virtual_host = VHost}) ->
    [{name, Name}, {vhost, VHost}].

permissions({VHost, Perms}) ->
    [{vhost, VHost}|permissions(Perms)];

permissions({User, Conf, Write, Read, Scope}) ->
    [{user, User}, {configure, Conf}, {write, Write}, {read, Read},
     {scope, Scope}].

user_permissions({VHost, Conf, Write, Read, Scope}) ->
    [{vhost, VHost}, {configure, Conf}, {write, Write}, {read, Read},
     {scope, Scope}].
