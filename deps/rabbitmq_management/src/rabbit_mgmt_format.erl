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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.

-module(rabbit_mgmt_format).

-export([format/2, print/2, remove/1, ip/1, ipb/1, amqp_table/1, tuple/1]).
-export([timestamp/1, timestamp_ms/1, strip_pids/1]).
-export([node_from_pid/1, protocol/1, resource/1, queue/1]).
-export([exchange/1, user/1, internal_user/1, binding/1, url/2]).
-export([pack_binding_props/2, unpack_binding_props/1, tokenise/1]).
-export([to_amqp_table/1, listener/1, properties/1, basic_properties/1]).
-export([record/2, to_basic_properties/1]).
-export([connection/1, addr/1, port/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

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

remove(_) -> [].

node_from_pid(Pid) when is_pid(Pid) -> [{node, node(Pid)}];
node_from_pid('')                   -> [];
node_from_pid(unknown)              -> [];
node_from_pid(none)                 -> [].

ip(unknown) -> unknown;
ip(IP)      -> list_to_binary(rabbit_misc:ntoa(IP)).

ipb(unknown) -> unknown;
ipb(IP)      -> list_to_binary(rabbit_misc:ntoab(IP)).

addr(Addr) when is_list(Addr); is_atom(Addr) -> print("~s", Addr);
addr(Addr) when is_tuple(Addr)               -> ip(Addr).

port(Port) when is_number(Port) -> Port;
port(Port)                      -> print("~w", Port).

properties(unknown) -> unknown;
properties(Table)   -> {struct, [{Name, tuple(Value)} ||
                                    {Name, Value} <- Table]}.

amqp_table(unknown)   -> unknown;
amqp_table(undefined) -> amqp_table([]);
amqp_table(Table)     -> {struct, [{Name, amqp_value(Type, Value)} ||
                                      {Name, Type, Value} <- Table]}.

amqp_value(array, Vs)                  -> [amqp_value(T, V) || {T, V} <- Vs];
amqp_value(table, V)                   -> amqp_table(V);
amqp_value(_Type, V) when is_binary(V) -> utf8_safe(V);
amqp_value(_Type, V)                   -> V.

utf8_safe(V) ->
    try
        xmerl_ucs:from_utf8(V),
        V
    catch exit:{ucs, _} ->
            Enc = base64:encode(V),
            <<"Invalid UTF-8, base64 is: ", Enc/binary>>
    end.

tuple(unknown)                    -> unknown;
tuple(Tuple) when is_tuple(Tuple) -> [tuple(E) || E <- tuple_to_list(Tuple)];
tuple(Term)                       -> Term.

protocol(unknown) ->
    unknown;
protocol(Version = {_Major, _Minor, _Revision}) ->
    protocol({'AMQP', Version});
protocol({Family, Version}) ->
    print("~s ~s", [Family, protocol_version(Version)]).

protocol_version(Arbitrary)
  when is_list(Arbitrary)                  -> Arbitrary;
protocol_version({Major, Minor})           -> io_lib:format("~B-~B", [Major, Minor]);
protocol_version({Major, Minor, 0})        -> protocol_version({Major, Minor});
protocol_version({Major, Minor, Revision}) -> io_lib:format("~B-~B-~B",
                                                    [Major, Minor, Revision]).

timestamp_ms(unknown) ->
    unknown;
timestamp_ms(Timestamp) ->
    timer:now_diff(Timestamp, {0,0,0}) div 1000.

timestamp(unknown) ->
    unknown;
timestamp(Timestamp) ->
    {{Y, M, D}, {H, Min, S}} = calendar:now_to_local_time(Timestamp),
    print("~w-~w-~w ~w:~w:~w", [Y, M, D, H, Min, S]).

resource(unknown) -> unknown;
resource(Res)     -> resource(name, Res).

resource(_, unknown) ->
    unknown;
resource(NameAs, #resource{name = Name, virtual_host = VHost}) ->
    [{NameAs, Name}, {vhost, VHost}].

internal_user(User) ->
    [{name,          User#internal_user.username},
     {password_hash, base64:encode(User#internal_user.password_hash)},
     {tags,          tags(User#internal_user.tags)}].

user(User) ->
    [{name,         User#user.username},
     {tags,         tags(User#user.tags)},
     {auth_backend, User#user.auth_backend}].

tags(Tags) ->
    list_to_binary(string:join([atom_to_list(T) || T <- Tags], ",")).

listener(#listener{node = Node, protocol = Protocol,
                   ip_address = IPAddress, port = Port}) ->
    [{node, Node},
     {protocol, Protocol},
     {ip_address, ip(IPAddress)},
     {port, Port}].

pack_binding_props(<<"">>, []) ->
    <<"_">>;
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
        {unquote_binding(Key), to_amqp_table(unpack_binding_args(Args))}
    catch E -> E
    end;
unpack_binding_props0([]) ->
    {bad_request, empty_properties}.

unpack_binding_args([]) ->
    [];
unpack_binding_args([K]) ->
    throw({bad_request, {no_value, K}});
unpack_binding_args([K, V | Rest]) ->
    [{unquote_binding(K), unquote_binding(V)} | unpack_binding_args(Rest)].

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

to_amqp_table({struct, T}) ->
    to_amqp_table(T);
to_amqp_table(T) ->
    [to_amqp_table_row(K, V) || {K, V} <- T].

to_amqp_table_row(K, V) ->
    {T, V2} = type_val(V),
    {K, T, V2}.

to_amqp_array(L) ->
    [type_val(I) || I <- L].

type_val({struct, M})         -> {table,   to_amqp_table(M)};
type_val(L) when is_list(L)   -> {array,   to_amqp_array(L)};
type_val(X) when is_binary(X) -> {longstr, X};
type_val(X) when is_number(X) -> {long,    X};
type_val(X)                   -> throw({unhandled_type, X}).

url(Fmt, Vals) ->
    print(Fmt, [mochiweb_util:quote_plus(V) || V <- Vals]).

connection(Props) ->
    case proplists:get_value(name, Props, unknown) of
        unknown -> print("~s:~w",
                         [addr(proplists:get_value(peer_address, Props)),
                          port(proplists:get_value(peer_port,    Props))]);
        Name      -> Name
    end.

exchange(X) ->
    format(X, [{fun resource/1,   [name]},
               {fun amqp_table/1, [arguments]}]).

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
      [{fun resource/1,     [name]},
       {fun amqp_table/1,   [arguments]}]).

%% We get bindings using rabbit_binding:list_*/1 rather than :info_all/1 since
%% there are no per-exchange / queue / etc variants for the latter. Therefore
%% we have a record rather than a proplist to deal with.
binding(#binding{source      = S,
                 key         = Key,
                 destination = D,
                 args        = Args}) ->
    format(
      [{source,           S},
       {destination,      D#resource.name},
       {destination_type, D#resource.kind},
       {routing_key,      Key},
       {arguments,        Args},
       {properties_key, pack_binding_props(Key, Args)}],
      [{fun (Res) -> resource(source, Res) end, [source]},
       {fun amqp_table/1,                       [arguments]}]).

basic_properties(Props = #'P_basic'{}) ->
    Res = record(Props, record_info(fields, 'P_basic')),
    format(Res, [{fun amqp_table/1, [headers]}]).

record(Record, Fields) ->
    {Res, _Ix} = lists:foldl(fun (K, {L, Ix}) ->
                                     V = element(Ix, Record),
                                     NewL = case V of
                                                undefined -> L;
                                                _         -> [{K, V}|L]
                                            end,
                                     {NewL, Ix + 1}
                             end, {[], 2}, Fields),
    Res.

to_basic_properties({struct, P}) ->
    to_basic_properties(P);

to_basic_properties(Props) ->
    Fmt = fun (headers, H) -> to_amqp_table(H);
              (_K     , V) -> V
          end,
    {Res, _Ix} = lists:foldl(
                   fun (K, {P, Ix}) ->
                           NewP = case proplists:get_value(a2b(K), Props) of
                                      undefined -> P;
                                      V         -> setelement(Ix, P, Fmt(K, V))
                                  end,
                           {NewP, Ix + 1}
                   end, {#'P_basic'{}, 2},
                   record_info(fields, 'P_basic')),
    Res.

a2b(A) ->
    list_to_binary(atom_to_list(A)).

%% Items can be connections, channels, consumers or queues, hence remove takes
%% various items.
strip_pids(Item = [T | _]) when is_tuple(T) ->
    format(Item,
           [{fun node_from_pid/1, [pid]},
            {fun remove/1,        [connection, owner_pid, queue, channel]},
            {fun mirror_nodes/1,  [slave_pids]}]);

strip_pids(Items) -> [strip_pids(I) || I <- Items].

mirror_nodes(Pids) ->
    [{mirror_nodes, [node(Pid) || Pid <- Pids]}].
