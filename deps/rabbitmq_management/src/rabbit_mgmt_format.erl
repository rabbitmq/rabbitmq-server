
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
-export([protocol/1, resource/1, permissions/1, queue/1]).
-export([exchange/1, user/1, binding/1, url/2, application/1]).
-export([pack_binding_props/2, unpack_binding_props/1, tokenise/1]).
-export([args_type/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

format(Stats, Fs) ->
    lists:concat([format_item(Stat, Fs) || {_Name, Value} = Stat <- Stats,
                                           Value =/= unknown]).

format_item(Stat, []) ->
    [Stat];
format_item({Name, Value}, [{Fun, Names} | Fs]) ->
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

%% TODO - can we remove all these "unknown" cases? Coverage never hits them.

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

protocol(unknown) ->
    unknown;
protocol(Version = {Major, Minor, Revision}) ->
    protocol({'AMQP', Version});
protocol({Family, Version}) -> print("~s ~s", [Family, protocol_version(Version)]).

protocol_version(Arbitrary)
  when is_list(Arbitrary)                  -> Arbitrary;
protocol_version({Major, Minor})           -> io_lib:format("~B-~B", [Major, Minor]);
protocol_version({Major, Minor, 0})        -> protocol_version({Major, Minor});
protocol_version({Major, Minor, Revision}) -> io_lib:format("~B-~B-~B",
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

permissions({User, VHost, Conf, Write, Read, Scope}) ->
    [{user,      User},
     {vhost,     VHost},
     {configure, Conf},
     {write,     Write},
     {read,      Read},
     {scope,     Scope}].

user(User) ->
    [{name,          User#user.username},
     {password,      User#user.password},
     {administrator, User#user.is_admin}].

pack_binding_props(Key, Args) ->
    Dict = dict:from_list([{K, V} || {K, _, V} <- Args]),
    ArgsKeys = lists:sort(dict:fetch_keys(Dict)),
    PackedArgs =
        string:join(
          [quote_binding(K) ++ "_" ++
               quote_binding(dict:fetch(K, Dict)) || K <- ArgsKeys],
          "_"),
    list_to_binary(quote_binding(Key) ++
                       case PackedArgs of
                           "" -> "";
                           _  -> "_" ++ PackedArgs
                       end).

quote_binding(Name) ->
    binary_to_list(
      iolist_to_binary(
        re:replace(mochiweb_util:quote_plus(Name), "_", "%5F"))).

unpack_binding_props(B) when is_binary(B) ->
    unpack_binding_props(binary_to_list(B));
unpack_binding_props(Str) ->
    unpack_binding_props0(tokenise(Str)).

unpack_binding_props0([Key | Args]) ->
    try
        {unquote_binding(Key), unpack_binding_args(Args)}
    catch E -> E
    end;
unpack_binding_props0([]) ->
    {bad_request, empty_properties}.

unpack_binding_args([]) ->
    [];
unpack_binding_args([K]) ->
    throw({bad_request, {no_value, K}});
unpack_binding_args([K, V | Rest]) ->
    Value = unquote_binding(V),
    [{unquote_binding(K), args_type(Value), Value} | unpack_binding_args(Rest)].

unquote_binding(Name) ->
    list_to_binary(mochiweb_util:unquote(Name)).

%% Unfortunately string:tokens("foo__bar", "_"). -> ["foo","bar"], we lose
%% the fact that there's a double _.
tokenise("") ->
    [];
tokenise(Str) ->
    Count = string:cspan(Str, "_"),
    case length(Str) of
        Count -> [Str];
        _     -> [string:sub_string(Str, 1, Count) |
                  tokenise(string:sub_string(Str, Count + 2))]
    end.

args_type(X) when is_binary(X) ->
    longstr;
args_type(X) when is_number(X) ->
    long;
args_type(X) ->
    throw({unhandled_type, X}).

url(Fmt, Vals) ->
    print(Fmt, [mochiweb_util:quote_plus(V) || V <- Vals]).

application({Application, Description, Version}) ->
    [{name, Application},
     {description, list_to_binary(Description)},
     {version, list_to_binary(Version)}].

exchange(X) ->
    format(X, [{fun resource/1, [name]},
               {fun table/1,    [arguments]}]).

%% We get queues using rabbit_amqqueue:list/1 rather than :info_all/1 since
%% the latter wakes up each queue. Therefore we have a record rather than a
%% proplist to deal with.
queue(#amqqueue{name            = Name,
                durable         = Durable,
                auto_delete     = AutoDelete,
                exclusive_owner = ExclusiveOwner,
                arguments       = Arguments,
                pid             = Pid }) ->
    format(
      [{name,        Name},
       {durable,     Durable},
       {auto_delete, AutoDelete},
       {owner_pid,   ExclusiveOwner},
       {arguments,   Arguments},
       {pid,         Pid}],
      [{fun pid/1,      [pid, owner_pid]},
       {fun resource/1, [name]},
       {fun table/1,    [arguments]}]).

%% We get bindings using rabbit_binding:list_*/1 rather than :info_all/1 since
%% there are no per-exchange / queue / etc variants for the latter. Therefore
%% we have a record rather than a proplist to deal with.
binding(#binding{exchange_name = X,
                 key           = Key,
                 queue_name    = Q,
                 args          = Args}) ->
    format(
      [{exchange,       X},
       {queue,          Q#resource.name},
       {routing_key,    Key},
       {arguments,      Args},
       {properties_key, pack_binding_props(Key, Args)}],
      [{fun (Res) -> resource(exchange, Res) end, [exchange]},
       {fun table/1,                              [arguments]}]).
