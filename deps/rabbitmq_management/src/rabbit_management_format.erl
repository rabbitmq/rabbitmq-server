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
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_format).

-export([format/2, print/2, pid/1, ip/1, table/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

format(Stats, Fs) ->
    [format_item(Stat, Fs) || Stat <- Stats].

format_item(Stat, []) ->
    Stat;
format_item(Stat, [F|Fs]) ->
    format_item(format_item0(Stat, F), Fs).

format_item0({Name, Value}, {Fun, Names}) ->
    case lists:member(Name, Names) of
        true -> {Name, Fun(Value)};
        _    -> {Name, Value}
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
    {struct, [{Name, Value} || {Name, _Type, Value} <- Table]}.
