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

-export([format/2, print/2, pid/1, ip/1, table/1, tuple/1, timestamp/1]).
-export([protocol/1, resource/1, permissions/1, user_permissions/1]).
-export([exchange/1, user/1, binding/1, pack_props/2, url/2]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

%% TODO rewrite using lists:concat([<filtering-list-comprehension>])
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
        true  -> case Fun(Value) of
                     List when is_list(List) -> List;
                     Formatted               -> [{Name, Formatted}]
                 end;
        false -> format_item({Name, Value}, Fs)
    end.

print(Fmt, Val) when is_list(Val) ->
    list_to_binary(lists:flatten(io_lib:format(Fmt, Val)));
print(Fmt, Val) ->
    print(Fmt, [Val]).

pid(Pid) when is_pid(Pid) -> list_to_binary(rabbit_misc:pid_to_string(Pid));
pid('')                   ->  <<"">>;
pid(unknown)              -> unknown;
pid(none)                 -> none.

ip(unknown) -> unknown;
ip(IP)      -> list_to_binary(inet_parse:ntoa(IP)).

table(unknown) -> unknown;
table(Table)   -> {struct, [{Name, tuple(Value)} ||
                               {Name, _Type, Value} <- Table]}.

tuple(unknown)                    -> unknown;
tuple(Tuple) when is_tuple(Tuple) -> [tuple(E) || E <- tuple_to_list(Tuple)];
tuple(Term)                       -> Term.

protocol(unknown)                  -> unknown;
protocol({Major, Minor, 0})        -> print("~w-~w", [Major, Minor]);
protocol({Major, Minor, Revision}) -> print("~w-~w-~w",
                                            [Major, Minor, Revision]).

timestamp(unknown) ->
    unknown;
timestamp(Timestamp) ->
    timer:now_diff(Timestamp, {0,0,0}) div 1000.

resource(unknown) -> unknown;
resource(Res)     -> resource(name, Res).

resource(_, unknown) ->
    unknown;
resource(NameAs, #resource{name = Name, virtual_host = VHost}) ->
    [{NameAs, Name}, {vhost, VHost}].

permissions({VHost, Perms}) ->
    [{vhost, VHost}|permissions(Perms)];

permissions({User, Conf, Write, Read}) ->
    [{user,      User},
     {configure, Conf},
     {write,     Write},
     {read,      Read}].

user_permissions({VHost, Conf, Write, Read}) ->
    [{vhost,     VHost},
     {configure, Conf},
     {write,     Write},
     {read,      Read}].

exchange(X) ->
    format(X, [{fun resource/1, [name]},
               {fun table/1, [arguments]}]).

user(User) ->
    [{name,          User#user.username},
     {password,      User#user.password},
     {administrator, User#user.is_admin}].

binding(#binding{exchange_name = X, key = Key, queue_name = Q, args = Args}) ->
    format([{exchange,       X},
            {queue,          Q#resource.name},
            {routing_key,    Key},
            {arguments,      Args},
            {properties_key, pack_props(Key, Args)}],
           [{fun (Res) -> resource(exchange, Res) end, [exchange]}]).

%% TODO
pack_props(Key, _Args) ->
    list_to_binary("key_" ++ mochiweb_util:quote_plus(binary_to_list(Key))).

url(Fmt, Vals) ->
    print(Fmt, [mochiweb_util:quote_plus(V) || V <- Vals]).
