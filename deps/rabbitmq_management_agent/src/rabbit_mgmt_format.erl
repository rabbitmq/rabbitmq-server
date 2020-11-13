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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_format).

-export([format/2, ip/1, ipb/1, amqp_table/1, tuple/1]).
-export([parameter/1, now_to_str/1, now_to_str_ms/1, strip_pids/1]).
-export([protocol/1, resource/1, queue/1, queue_state/1]).
-export([exchange/1, user/1, internal_user/1, binding/1, url/2]).
-export([pack_binding_props/2, tokenise/1]).
-export([to_amqp_table/1, listener/1, web_context/1, properties/1, basic_properties/1]).
-export([record/2, to_basic_properties/1]).
-export([addr/1, port/1]).
-export([format_nulls/1, escape_html_tags/1]).
-export([print/2, print/1]).

-export([format_queue_stats/1, format_channel_stats/1,
         format_consumer_arguments/1,
         format_connection_created/1,
         format_accept_content/1, format_args/1]).

-export([strip_queue_pids/1]).

-export([clean_consumer_details/1, clean_channel_details/1]).

-export([args_hash/1]).

-import(rabbit_misc, [pget/2, pget/3, pset/3]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

%%--------------------------------------------------------------------

format(Stats, {[], _}) ->
    [Stat || {_Name, Value} = Stat <- Stats, Value =/= unknown];
format(Stats, {Fs, true}) ->
    [Fs(Stat) || {_Name, Value} = Stat <- Stats, Value =/= unknown];
format(Stats, {Fs, false}) ->
    lists:concat([Fs(Stat) || {_Name, Value} = Stat <- Stats,
                              Value =/= unknown]).

format_queue_stats({reductions, _}) ->
    [];
format_queue_stats({exclusive_consumer_pid, _}) ->
    [];
format_queue_stats({slave_pids, ''}) ->
    [];
format_queue_stats({slave_pids, Pids}) ->
    [{slave_nodes, [node(Pid) || Pid <- Pids]}];
format_queue_stats({synchronised_slave_pids, ''}) ->
    [];
format_queue_stats({effective_policy_definition, []}) ->
    [{effective_policy_definition, #{}}];
format_queue_stats({synchronised_slave_pids, Pids}) ->
    [{synchronised_slave_nodes, [node(Pid) || Pid <- Pids]}];
format_queue_stats({backing_queue_status, Value}) ->
    [{backing_queue_status, properties(Value)}];
format_queue_stats({idle_since, Value}) ->
    [{idle_since, now_to_str(Value)}];
format_queue_stats({state, Value}) ->
    queue_state(Value);
format_queue_stats({disk_reads, _}) ->
    [];
format_queue_stats({disk_writes, _}) ->
    [];
format_queue_stats(Stat) ->
    [Stat].

format_channel_stats([{idle_since, Value} | Rest]) ->
    [{idle_since, now_to_str(Value)} | Rest];
format_channel_stats(Stats) ->
    Stats.

%% Conerts an HTTP API request payload value
%% to AMQP 0-9-1 arguments table
format_args({arguments, []}) ->
    {arguments, []};
format_args({arguments, Value}) ->
    {arguments, to_amqp_table(Value)};
format_args(Stat) ->
    Stat.

format_connection_created({host, Value}) ->
    {host, addr(Value)};
format_connection_created({peer_host, Value}) ->
    {peer_host, addr(Value)};
format_connection_created({port, Value}) ->
    {port, port(Value)};
format_connection_created({peer_port, Value}) ->
    {peer_port, port(Value)};
format_connection_created({protocol, Value}) ->
    {protocol, protocol(Value)};
format_connection_created({client_properties, Value}) ->
    {client_properties, amqp_table(Value)};
format_connection_created(Stat) ->
    Stat.

format_exchange_and_queue({policy, Value}) ->
    policy(Value);
format_exchange_and_queue({arguments, Value}) ->
    [{arguments, amqp_table(Value)}];
format_exchange_and_queue({name, Value}) ->
    resource(Value);
format_exchange_and_queue(Stat) ->
    [Stat].

format_binding({source, Value}) ->
    resource(source, Value);
format_binding({arguments, Value}) ->
    [{arguments, amqp_table(Value)}];
format_binding(Stat) ->
    [Stat].

format_basic_properties({headers, Value}) ->
    {headers, amqp_table(Value)};
format_basic_properties(Stat) ->
    Stat.

format_accept_content({durable, Value}) ->
    {durable, parse_bool(Value)};
format_accept_content({auto_delete, Value}) ->
    {auto_delete, parse_bool(Value)};
format_accept_content({internal, Value}) ->
    {internal, parse_bool(Value)};
format_accept_content(Stat) ->
    Stat.

print(Fmt, Val) when is_list(Val) ->
    list_to_binary(lists:flatten(io_lib:format(Fmt, Val)));
print(Fmt, Val) ->
    print(Fmt, [Val]).

print(Val) when is_list(Val) ->
    list_to_binary(lists:flatten(Val));
print(Val) ->
    Val.

%% TODO - can we remove all these "unknown" cases? Coverage never hits them.

ip(unknown) -> unknown;
ip(IP)      -> list_to_binary(rabbit_misc:ntoa(IP)).

ipb(unknown) -> unknown;
ipb(IP)      -> list_to_binary(rabbit_misc:ntoab(IP)).

addr(S)    when is_list(S); is_atom(S); is_binary(S) -> print("~s", S);
addr(Addr) when is_tuple(Addr)                       -> ip(Addr).

port(Port) when is_number(Port) -> Port;
port(Port)                      -> print("~w", Port).

properties(unknown) -> unknown;
properties(Table)   -> maps:from_list([{Name, tuple(Value)} ||
                                          {Name, Value} <- Table]).

amqp_table(unknown)   -> unknown;
amqp_table(undefined) -> amqp_table([]);
amqp_table([])        -> #{};
amqp_table(#{})       -> #{};
amqp_table(Table)     -> maps:from_list([{Name, amqp_value(Type, Value)} ||
                                            {Name, Type, Value} <- Table]).

amqp_value(array, Vs)                  -> [amqp_value(T, V) || {T, V} <- Vs];
amqp_value(table, V)                   -> amqp_table(V);
amqp_value(decimal, {Before, After})   ->
    erlang:list_to_float(
      lists:flatten(io_lib:format("~p.~p", [Before, After])));
amqp_value(_Type, V) when is_binary(V) -> utf8_safe(V);
amqp_value(_Type, V)                   -> V.

utf8_safe(V) ->
    try
        _ = xmerl_ucs:from_utf8(V),
        V
    catch exit:{ucs, _} ->
            Enc = split_lines(base64:encode(V)),
            <<"Not UTF-8, base64 is: ", Enc/binary>>
    end.

% MIME enforces a limit on line length of base 64-encoded data to 76 characters.
split_lines(<<Text:76/binary, Rest/binary>>) ->
    <<Text/binary, $\n, (split_lines(Rest))/binary>>;
split_lines(Text) ->
    Text.

parameter(P) -> pset(value, pget(value, P), P).

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

now_to_str(unknown) ->
    unknown;
now_to_str(MilliSeconds) ->
    BaseDate = calendar:datetime_to_gregorian_seconds({{1970, 1, 1},
                                                       {0, 0, 0}}),
    Seconds = BaseDate + (MilliSeconds div 1000),
    {{Y, M, D}, {H, Min, S}} = calendar:gregorian_seconds_to_datetime(Seconds),
    print("~w-~2.2.0w-~2.2.0w ~w:~2.2.0w:~2.2.0w", [Y, M, D, H, Min, S]).

now_to_str_ms(unknown) ->
    unknown;
now_to_str_ms(MilliSeconds) ->
    print("~s:~3.3.0w", [now_to_str(MilliSeconds), MilliSeconds rem 1000]).

resource(unknown) -> unknown;
resource(Res)     -> resource(name, Res).

resource(_, unknown) ->
    unknown;
resource(NameAs, #resource{name = Name, virtual_host = VHost}) ->
    [{NameAs, Name}, {vhost, VHost}].

policy('')     -> [];
policy(Policy) -> [{policy, Policy}].

internal_user(User) ->
    [{name,              User#internal_user.username},
     {password_hash,     base64:encode(User#internal_user.password_hash)},
     {hashing_algorithm, rabbit_auth_backend_internal:hashing_module_for_user(
                             User)},
     {tags,              tags(User#internal_user.tags)}].

user(User) ->
    [{name, User#user.username},
     {tags, tags(User#user.tags)}].

tags(Tags) ->
    list_to_binary(string:join([atom_to_list(T) || T <- Tags], ",")).

listener(#listener{node = Node, protocol = Protocol,
                   ip_address = IPAddress, port = Port, opts=Opts}) ->
    [{node, Node},
     {protocol, Protocol},
     {ip_address, ip(IPAddress)},
     {port, Port},
     {socket_opts, format_socket_opts(Opts)}].

web_context(Props0) ->
    SslOpts = pget(ssl_opts, Props0, []),
    Props   = proplists:delete(ssl_opts, Props0),
    [{ssl_opts, format_socket_opts(SslOpts)} | Props].

format_socket_opts(Opts) ->
    format_socket_opts(Opts, []).

format_socket_opts([], Acc) ->
    lists:reverse(Acc);
%% for HTTP API listeners this will be included into
%% socket_opts
format_socket_opts([{ssl_opts, Value} | Tail], Acc) ->
    format_socket_opts(Tail, [{ssl_opts, format_socket_opts(Value)} | Acc]);
%% exclude options that have values that are nested
%% data structures or may include functions. They are fairly
%% obscure and not worth reporting via HTTP API.
format_socket_opts([{verify_fun, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([{crl_cache, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([{partial_chain, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([{user_lookup_fun, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([{sni_fun, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
%% we do not report SNI host details in the UI,
%% so skip this option and avoid some recursive formatting
%% complexity
format_socket_opts([{sni_hosts, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([{reuse_session, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
%% we do not want to report configured cipher suites, even
%% though formatting them is straightforward
format_socket_opts([{ciphers, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
%% single atom options, e.g. `binary`
format_socket_opts([Head | Tail], Acc) when is_atom(Head) ->
    format_socket_opts(Tail, [{Head, true} | Acc]);
%% verify_fun value is a tuple that includes a function
format_socket_opts([_Head = {verify_fun, _Value} | Tail], Acc) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([Head = {Name, Value} | Tail], Acc) when is_list(Value) ->
    case io_lib:printable_unicode_list(Value) of
        true -> format_socket_opts(Tail, [{Name, unicode:characters_to_binary(Value)} | Acc]);
        false -> format_socket_opts(Tail, [Head | Acc])
    end;
format_socket_opts([{Name, Value} | Tail], Acc) when is_tuple(Value) ->
    format_socket_opts(Tail, [{Name, tuple_to_list(Value)} | Acc]);
%% exclude functions from JSON encoding
format_socket_opts([_Head = {_Name, Value} | Tail], Acc) when is_function(Value) ->
    format_socket_opts(Tail, Acc);
format_socket_opts([Head | Tail], Acc) ->
    format_socket_opts(Tail, [Head | Acc]).

pack_binding_props(<<"">>, []) ->
    <<"~">>;
pack_binding_props(Key, []) ->
    list_to_binary(quote_binding(Key));
pack_binding_props(Key, Args) ->
    ArgsEnc = args_hash(Args),
    list_to_binary(quote_binding(Key) ++ "~" ++ quote_binding(ArgsEnc)).

quote_binding(Name) ->
    re:replace(rabbit_http_util:quote_plus(Name), "~", "%7E", [global]).

%% Unfortunately string:tokens("foo~~bar", "~"). -> ["foo","bar"], we lose
%% the fact that there's a double ~.
tokenise("") ->
    [];
tokenise(Str) ->
    Count = string:cspan(Str, "~"),
    case length(Str) of
        Count -> [Str];
        _     -> [string:sub_string(Str, 1, Count) |
                  tokenise(string:sub_string(Str, Count + 2))]
    end.

to_amqp_table(M) when is_map(M) ->
    lists:reverse(maps:fold(fun(K, V, Acc) -> [to_amqp_table_row(K, V)|Acc] end,
                            [], M));
to_amqp_table(L) when is_list(L) ->
    L.

to_amqp_table_row(K, V) ->
    {T, V2} = type_val(V),
    {K, T, V2}.

to_amqp_array(L) ->
    [type_val(I) || I <- L].

type_val(M) when is_map(M)     -> {table,   to_amqp_table(M)};
type_val(L) when is_list(L)    -> {array,   to_amqp_array(L)};
type_val(X) when is_binary(X)  -> {longstr, X};
type_val(X) when is_integer(X) -> {long,    X};
type_val(X) when is_number(X)  -> {double,  X};
type_val(true)                 -> {bool, true};
type_val(false)                -> {bool, false};
type_val(null)                 -> throw({error, null_not_allowed});
type_val(X)                    -> throw({error, {unhandled_type, X}}).

url(Fmt, Vals) ->
    print(Fmt, [rabbit_http_util:quote_plus(V) || V <- Vals]).

exchange(X) ->
    format(X, {fun format_exchange_and_queue/1, false}).

%% We get queues using rabbit_amqqueue:list/1 rather than :info_all/1 since
%% the latter wakes up each queue. Therefore we have a record rather than a
%% proplist to deal with.
queue(#amqqueue{name            = Name,
                durable         = Durable,
                auto_delete     = AutoDelete,
                exclusive_owner = ExclusiveOwner,
                arguments       = Arguments,
                pid             = Pid,
                state           = State}) ->
    format(
      [{name,        Name},
       {durable,     Durable},
       {auto_delete, AutoDelete},
       {exclusive,   is_pid(ExclusiveOwner)},
       {owner_pid,   ExclusiveOwner},
       {arguments,   Arguments},
       {pid,         Pid},
       {state,       State}],
      {fun format_exchange_and_queue/1, false}).

queue_state({syncing, Msgs}) -> [{state,         syncing},
                                 {sync_messages, Msgs}];
queue_state({terminated_by, Name}) ->
                                [{state, terminated},
                                 {terminated_by, Name}];
queue_state(Status)          -> [{state,         Status}].

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
      {fun format_binding/1, false}).

basic_properties(Props = #'P_basic'{}) ->
    Res = record(Props, record_info(fields, 'P_basic')),
    format(Res, {fun format_basic_properties/1, true}).

record(Record, Fields) ->
    {Res, _Ix} = lists:foldl(fun (K, {L, Ix}) ->
                                     {case element(Ix, Record) of
                                          undefined -> L;
                                          V         -> [{K, V}|L]
                                      end, Ix + 1}
                             end, {[], 2}, Fields),
    Res.

to_basic_properties(Props) when is_map(Props) ->
    E = fun err/2,
    Fmt = fun (headers,       H)                    -> to_amqp_table(H);
              (delivery_mode, V) when is_integer(V) -> V;
              (delivery_mode, _V)                   -> E(not_int,delivery_mode);
              (priority,      V) when is_integer(V) -> V;
              (priority,      _V)                   -> E(not_int, priority);
              (timestamp,     V) when is_integer(V) -> V;
              (timestamp,     _V)                   -> E(not_int, timestamp);
              (_,             V) when is_binary(V)  -> V;
              (K,            _V)                    -> E(not_string, K)
          end,
    {Res, _Ix} = lists:foldl(
                   fun (K, {P, Ix}) ->
                           {case maps:get(a2b(K), Props, undefined) of
                                undefined -> P;
                                V         -> setelement(Ix, P, Fmt(K, V))
                            end, Ix + 1}
                   end, {#'P_basic'{}, 2},
                   record_info(fields, 'P_basic')),
    Res.

-spec err(term(), term()) -> no_return().
err(A, B) ->
    throw({error, {A, B}}).

a2b(A) ->
    list_to_binary(atom_to_list(A)).

strip_queue_pids(Item) ->
    strip_queue_pids(Item, []).

strip_queue_pids([{_, unknown} | T], Acc) ->
    strip_queue_pids(T, Acc);
strip_queue_pids([{pid, Pid} | T], Acc0) when is_pid(Pid) ->
    Acc = case proplists:is_defined(node, Acc0) of
              false -> [{node, node(Pid)} | Acc0];
              true  -> Acc0
          end,
    strip_queue_pids(T, Acc);
strip_queue_pids([{pid, _} | T], Acc) ->
    strip_queue_pids(T, Acc);
strip_queue_pids([{owner_pid, _} | T], Acc) ->
    strip_queue_pids(T, Acc);
strip_queue_pids([Any | T], Acc) ->
    strip_queue_pids(T, [Any | Acc]);
strip_queue_pids([], Acc) ->
    Acc.

%% Items can be connections, channels, consumers or queues, hence remove takes
%% various items.
strip_pids(Item = [T | _]) when is_tuple(T) ->
    lists:usort(strip_pids(Item, []));

strip_pids(Items) -> [lists:usort(strip_pids(I)) || I <- Items].

strip_pids([{_, unknown} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{pid, Pid} | T], Acc) when is_pid(Pid) ->
    strip_pids(T, [{node, node(Pid)} | Acc]);
strip_pids([{pid, _} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{connection, _} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{owner_pid, _} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{channel, _} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{channel_pid, _} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{exclusive_consumer_pid, _} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{slave_pids, ''} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{slave_pids, Pids} | T], Acc) ->
    strip_pids(T, [{slave_nodes, [node(Pid) || Pid <- Pids]} | Acc]);
strip_pids([{synchronised_slave_pids, ''} | T], Acc) ->
    strip_pids(T, Acc);
strip_pids([{synchronised_slave_pids, Pids} | T], Acc) ->
    strip_pids(T, [{synchronised_slave_nodes, [node(Pid) || Pid <- Pids]} | Acc]);
strip_pids([{K, [P|_] = Nested} | T], Acc) when is_tuple(P) -> % recurse
    strip_pids(T, [{K, strip_pids(Nested)} | Acc]);
strip_pids([{K, [L|_] = Nested} | T], Acc) when is_list(L) -> % recurse
    strip_pids(T, [{K, strip_pids(Nested)} | Acc]);
strip_pids([Any | T], Acc) ->
    strip_pids(T, [Any | Acc]);
strip_pids([], Acc) ->
    Acc.

%% Format for JSON replies. Transforms '' into null
format_nulls(Items) when is_list(Items) ->
    [format_null_item(Pair) || Pair <- Items];
format_nulls(Item) ->
    format_null_item(Item).

format_null_item({Key, ''}) ->
    {Key, null};
format_null_item({Key, Value}) when is_list(Value) ->
    {Key, format_nulls(Value)};
format_null_item({Key, Value}) ->
    {Key, Value};
format_null_item([{_K, _V} | _T] = L) ->
    format_nulls(L);
format_null_item(Value) ->
    Value.


-spec escape_html_tags(string()) -> binary().

escape_html_tags(S) ->
    escape_html_tags(rabbit_data_coercion:to_list(S), []).


-spec escape_html_tags(string(), string()) -> binary().

escape_html_tags([], Acc) ->
    rabbit_data_coercion:to_binary(lists:reverse(Acc));
escape_html_tags("<" ++ Rest, Acc) ->
    escape_html_tags(Rest, lists:reverse("&lt;", Acc));
escape_html_tags(">" ++ Rest, Acc) ->
    escape_html_tags(Rest, lists:reverse("&gt;", Acc));
escape_html_tags("&" ++ Rest, Acc) ->
    escape_html_tags(Rest, lists:reverse("&amp;", Acc));
escape_html_tags([C | Rest], Acc) ->
    escape_html_tags(Rest, [C | Acc]).


-spec clean_consumer_details(proplists:proplist()) -> proplists:proplist().
clean_consumer_details(Obj) ->
     case pget(consumer_details, Obj) of
         undefined -> Obj;
         Cds ->
             Cons = [format_consumer_arguments(clean_channel_details(Con)) || Con <- Cds],
             pset(consumer_details, Cons, Obj)
     end.

-spec clean_channel_details(proplists:proplist()) -> proplists:proplist().
clean_channel_details(Obj) ->
    Obj0 = lists:keydelete(channel_pid, 1, Obj),
    case pget(channel_details, Obj0) of
         undefined -> Obj0;
         Chd ->
             pset(channel_details,
                  lists:keydelete(pid, 1, Chd),
                  Obj0)
     end.

-spec format_consumer_arguments(proplists:proplist()) -> proplists:proplist().
format_consumer_arguments(Obj) ->
    case pget(arguments, Obj) of
         undefined -> Obj;
         #{}       -> Obj;
         []        -> pset(arguments, #{}, Obj);
         Args      -> pset(arguments, amqp_table(Args), Obj)
     end.


parse_bool(<<"true">>)  -> true;
parse_bool(<<"false">>) -> false;
parse_bool(true)        -> true;
parse_bool(false)       -> false;
parse_bool(undefined)   -> undefined;
parse_bool(V)           -> throw({error, {not_boolean, V}}).

args_hash(Args) ->
    list_to_binary(rabbit_misc:base64url(<<(erlang:phash2(Args, 1 bsl 32)):32>>)).
