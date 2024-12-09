%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_misc).

-ignore_xref([{maps, get, 2}]).

-include("rabbit.hrl").
-include("rabbit_misc.hrl").

-include_lib("kernel/include/file.hrl").

-ifdef(TEST).
-export([decompose_pid/1, compose_pid/4]).
-endif.

-export([method_record_type/1, polite_pause/0, polite_pause/1]).
-export([die/1, frame_error/2, amqp_error/4, quit/1,
         protocol_error/3, protocol_error/4, protocol_error/1,
         precondition_failed/1, precondition_failed/2]).
-export([type_class/1, assert_args_equivalence/4, assert_field_equivalence/4]).
-export([table_lookup/2, set_table_value/4, amqp_table/1, to_amqp_table/1]).
-export([r/3, r/2, r_arg/4, rs/1,
         queue_resource/2, exchange_resource/2]).
-export([throw_on_error/2, with_exit_handler/2, is_abnormal_exit/1,
         filter_exit_map/2]).
-export([ensure_ok/2]).
-export([tcp_name/3, format_inet_error/1]).
-export([upmap/2, map_in_order/2, utf8_safe/1]).
-export([dirty_dump_log/1]).
-export([format/2, format_many/1, format_stderr/2]).
-export([unfold/2, ceil/1, queue_fold/3]).
-export([sort_field_table/1]).
-export([parse_bool/1, parse_int/1]).
-export([pid_to_string/1, string_to_pid/1,
         pid_change_node/2, node_to_fake_pid/1]).
-export([hexify/1]).
-export([version_compare/2, version_compare/3]).
-export([strict_version_minor_equivalent/2]).
-export([dict_cons/3, orddict_cons/3, maps_cons/3, gb_trees_cons/3]).
-export([gb_trees_fold/3, gb_trees_foreach/2]).
-export([all_module_attributes/1,
         rabbitmq_related_apps/0,
         rabbitmq_related_module_attributes/1,
         module_attributes_from_apps/2,
         build_acyclic_graph/3]).
-export([const/1]).
-export([ntoa/1, ntoab/1]).
-export([is_process_alive/1,
         process_info/2]).
-export([pget/2, pget/3, pupdate/3, pget_or_die/2, pmerge/3, pset/3, plmerge/2]).
-export([deep_pget/2, deep_pget/3]).
-export([format_message_queue/2]).
-export([append_rpc_all_nodes/4, append_rpc_all_nodes/5]).
-export([os_cmd/1, pwsh_cmd/1, win32_cmd/2]).
-export([is_os_process_alive/1]).
-export([version/0, otp_release/0, platform_and_version/0, otp_system_version/0,
         rabbitmq_and_erlang_versions/0, which_applications/0]).
-export([sequence_error/1]).
-export([check_expiry/1]).
-export([base64url/1]).
-export([interval_operation/5]).
-export([ensure_timer/4, stop_timer/2, send_after/3, cancel_timer/1]).
-export([get_parent/0]).
-export([store_proc_name/1, store_proc_name/2, get_proc_name/0]).
-export([moving_average/4]).
-export([escape_html_tags/1, b64decode_or_throw/1]).
-export([get_env/3]).
-export([get_channel_operation_timeout/0]).
-export([random/1]).
-export([rpc_call/4, rpc_call/5]).
-export([get_gc_info/1]).
-export([group_proplists_by/2]).
-export([raw_read_file/1]).
-export([find_child/2]).
-export([is_regular_file/1]).
-export([safe_ets_update_counter/3, safe_ets_update_counter/4, safe_ets_update_counter/5,
         safe_ets_update_element/3, safe_ets_update_element/4, safe_ets_update_element/5]).
-export([is_even/1, is_odd/1]).

-export([maps_any/2,
         maps_put_truthy/3,
         maps_put_falsy/3
        ]).
-export([remote_sup_child/2]).
-export([for_each_while_ok/2, fold_while_ok/3]).

%% Horrible macro to use in guards
-define(IS_BENIGN_EXIT(R),
        R =:= noproc; R =:= noconnection; R =:= nodedown; R =:= normal;
            R =:= shutdown).

%%----------------------------------------------------------------------------

-export_type([resource_name/0, thunk/1, channel_or_connection_exit/0]).

-type ok_or_error() :: rabbit_types:ok_or_error(any()).
-type thunk(T) :: fun(() -> T).
-type resource_name() :: binary().
-type channel_or_connection_exit()
      :: rabbit_types:channel_exit() | rabbit_types:connection_exit().
-type digraph_label() :: term().
-type graph_vertex_fun() ::
        fun (({atom(), [term()]}) -> [{digraph:vertex(), digraph_label()}]).
-type graph_edge_fun() ::
        fun (({atom(), [term()]}) -> [{digraph:vertex(), digraph:vertex()}]).
-type tref() :: {'erlang', reference()} | {timer, timer:tref()}.

-spec method_record_type(rabbit_framing:amqp_method_record()) ->
          rabbit_framing:amqp_method_name().
-spec polite_pause() -> 'done'.
-spec polite_pause(non_neg_integer()) -> 'done'.
-spec die(rabbit_framing:amqp_exception()) -> channel_or_connection_exit().

-spec quit(integer()) -> no_return().

-spec frame_error(rabbit_framing:amqp_method_name(), binary()) ->
          rabbit_types:connection_exit().
-spec amqp_error
        (rabbit_framing:amqp_exception(), string(), [any()],
         rabbit_framing:amqp_method_name()) ->
            rabbit_types:amqp_error().
-spec protocol_error(rabbit_framing:amqp_exception(), string(), [any()]) ->
          channel_or_connection_exit().
-spec protocol_error
        (rabbit_framing:amqp_exception(), string(), [any()],
         rabbit_framing:amqp_method_name()) ->
            channel_or_connection_exit().
-spec protocol_error(rabbit_types:amqp_error()) ->
          channel_or_connection_exit().
-spec type_class(rabbit_framing:amqp_field_type()) -> atom().
-spec assert_args_equivalence
        (rabbit_framing:amqp_table(), rabbit_framing:amqp_table(),
         rabbit_types:r(any()), [binary()]) ->
            'ok' | rabbit_types:connection_exit().
-spec assert_field_equivalence
        (any(), any(), rabbit_types:r(any()), atom() | binary()) ->
            'ok' | rabbit_types:connection_exit().
-spec equivalence_fail
        (any(), any(), rabbit_types:r(any()), atom() | binary()) ->
            rabbit_types:connection_exit().
-spec table_lookup(rabbit_framing:amqp_table(), binary()) ->
    'undefined' | {rabbit_framing:amqp_field_type(), rabbit_framing:amqp_value()}.
-spec set_table_value
        (rabbit_framing:amqp_table(), binary(), rabbit_framing:amqp_field_type(),
         rabbit_framing:amqp_value()) ->
            rabbit_framing:amqp_table().
-spec r(rabbit_types:vhost(), K) ->
          rabbit_types:r3(rabbit_types:vhost(), K, '_')
          when is_subtype(K, atom()).
-spec r(rabbit_types:vhost() | rabbit_types:r(atom()), K, resource_name()) ->
          rabbit_types:r3(rabbit_types:vhost(), K, resource_name())
          when is_subtype(K, atom()).
-spec r_arg
        (rabbit_types:vhost() | rabbit_types:r(atom()), K,
         rabbit_framing:amqp_table(), binary()) ->
            undefined |
            rabbit_types:error(
              {invalid_type, rabbit_framing:amqp_field_type()}) |
            rabbit_types:r(K) when is_subtype(K, atom()).
-spec rs(rabbit_types:r(atom())) -> string().
-spec throw_on_error
        (atom(), thunk(rabbit_types:error(any()) | {ok, A} | A)) -> A.
-spec with_exit_handler(thunk(A), thunk(A)) -> A.
-spec is_abnormal_exit(any()) -> boolean().
-spec filter_exit_map(fun ((A) -> B), [A]) -> [B].
-spec ensure_ok(ok_or_error(), atom()) -> 'ok'.
-spec tcp_name(atom(), inet:ip_address(), rabbit_net:ip_port()) ->
          atom().
-spec format_inet_error(atom()) -> string().
-spec upmap(fun ((A) -> B), [A]) -> [B].
-spec map_in_order(fun ((A) -> B), [A]) -> [B].
-spec dirty_dump_log(file:filename()) -> ok_or_error().
-spec format(string(), [any()]) -> string().
-spec format_many([{string(), [any()]}]) -> string().
-spec format_stderr(string(), [any()]) -> 'ok'.
-spec unfold (fun ((A) -> ({'true', B, A} | 'false')), A) -> {[B], A}.
-spec ceil(number()) -> integer().
-spec queue_fold(fun ((any(), B) -> B), B, queue:queue()) -> B.
-spec sort_field_table(rabbit_framing:amqp_table()) ->
          rabbit_framing:amqp_table().
-spec pid_to_string(pid()) -> string().
-spec string_to_pid(string()) -> pid().
-spec pid_change_node(pid(), node()) -> pid().
-spec node_to_fake_pid(atom()) -> pid().
-spec version_compare(string(), string()) -> 'lt' | 'eq' | 'gt'.
-spec version_compare
        (rabbit_semver:version_string(), rabbit_semver:version_string(),
         ('lt' | 'lte' | 'eq' | 'gte' | 'gt')) -> boolean().
-spec dict_cons(any(), any(), dict:dict()) -> dict:dict().
-spec orddict_cons(any(), any(), orddict:orddict()) -> orddict:orddict().
-spec gb_trees_cons(any(), any(), gb_trees:tree()) -> gb_trees:tree().
-spec gb_trees_fold(fun ((any(), any(), A) -> A), A, gb_trees:tree()) -> A.
-spec gb_trees_foreach(fun ((any(), any()) -> any()), gb_trees:tree()) ->
          'ok'.
-spec all_module_attributes(atom()) -> [{atom(), atom(), [term()]}].
-spec build_acyclic_graph
        (graph_vertex_fun(), graph_edge_fun(), [{atom(), [term()]}]) ->
            rabbit_types:ok_or_error2(
              digraph:graph(),
              {'vertex', 'duplicate', digraph:vertex()} |
              {'edge',
                ({bad_vertex, digraph:vertex()} |
                 {bad_edge, [digraph:vertex()]}),
                digraph:vertex(), digraph:vertex()}).
-spec const(A) -> thunk(A).
-spec ntoa(inet:ip_address()) -> string().
-spec ntoab(inet:ip_address()) -> string().
-spec is_process_alive(pid()) -> boolean().

-spec pmerge(term(), term(), [term()]) -> [term()].
-spec plmerge([term()], [term()]) -> [term()].
-spec pset(term(), term(), [term()]) -> [term()].
-spec format_message_queue(any(), priority_queue:q()) -> term().
-spec os_cmd(string()) -> string().
-spec is_os_process_alive(non_neg_integer() | string()) -> boolean().
-spec version() -> string().
-spec otp_release() -> string().
-spec otp_system_version() -> string().
-spec platform_and_version() -> string().
-spec rabbitmq_and_erlang_versions() -> {string(), string()}.
-spec which_applications() -> [{atom(), string(), string()}].
-spec sequence_error([({'error', any()} | any())]) ->
          {'error', any()} | any().
-spec check_expiry(integer()) -> rabbit_types:ok_or_error(any()).
-spec base64url(binary()) -> string().
-spec interval_operation
        ({atom(), atom(), any()}, float(), non_neg_integer(), non_neg_integer(),
         non_neg_integer()) ->
            {any(), non_neg_integer()}.
-spec ensure_timer(A, non_neg_integer(), non_neg_integer(), any()) -> A.
-spec stop_timer(A, non_neg_integer()) -> A.
-spec send_after(non_neg_integer(), pid(), any()) -> tref().
-spec cancel_timer(tref()) -> 'ok'.
-spec get_parent() -> pid().
-spec store_proc_name(atom(), rabbit_types:proc_name()) -> ok.
-spec store_proc_name(rabbit_types:proc_type_and_name()) -> ok.
-spec get_proc_name() -> rabbit_types:proc_name().
-spec moving_average(float(), float(), float(), float() | 'undefined') ->
          float().
-spec get_env(atom(), atom(), term())  -> term().
-spec get_channel_operation_timeout() -> non_neg_integer().
-spec random(non_neg_integer()) -> non_neg_integer().
-spec get_gc_info(pid()) -> [any()].
-spec group_proplists_by(fun((proplists:proplist()) -> any()),
                         list(proplists:proplist())) -> list(list(proplists:proplist())).

-spec precondition_failed(string()) -> no_return().
-spec precondition_failed(string(), [any()]) -> no_return().

%%----------------------------------------------------------------------------

method_record_type(Record) ->
    element(1, Record).

polite_pause() ->
    polite_pause(3000).

polite_pause(N) ->
    receive
    after N -> done
    end.

die(Error) ->
    protocol_error(Error, "~w", [Error]).

frame_error(MethodName, BinaryFields) ->
    protocol_error(frame_error, "cannot decode ~w", [BinaryFields], MethodName).

amqp_error(Name, ExplanationFormat, Params, Method) ->
    Explanation = format(ExplanationFormat, Params),
    #amqp_error{name = Name, explanation = Explanation, method = Method}.

protocol_error(Name, ExplanationFormat, Params) ->
    protocol_error(Name, ExplanationFormat, Params, none).

protocol_error(Name, ExplanationFormat, Params, Method) ->
    protocol_error(amqp_error(Name, ExplanationFormat, Params, Method)).

protocol_error(#amqp_error{} = Error) ->
    exit(Error).

precondition_failed(Format) -> precondition_failed(Format, []).

precondition_failed(Format, Params) ->
    protocol_error(precondition_failed, Format, Params).

type_class(byte)          -> int;
type_class(short)         -> int;
type_class(signedint)     -> int;
type_class(long)          -> int;
type_class(decimal)       -> int;
type_class(unsignedbyte)  -> int;
type_class(unsignedshort) -> int;
type_class(unsignedint)   -> int;
type_class(float)         -> float;
type_class(double)        -> float;
type_class(Other)         -> Other.

assert_args_equivalence(Orig, New, Name, Keys) ->
    [assert_args_equivalence1(Orig, New, Name, Key) || Key <- Keys],
    ok.

assert_args_equivalence1(Orig, New, Name, Key) ->
    {Orig1, New1} = {table_lookup(Orig, Key), table_lookup(New, Key)},
    case {Orig1, New1} of
        {Same, Same} ->
            ok;
        {{OrigType, OrigVal}, {NewType, NewVal}} ->
            case type_class(OrigType) == type_class(NewType) andalso
                 OrigVal == NewVal of
                 true  -> ok;
                 false -> assert_field_equivalence(OrigVal, NewVal, Name, Key)
            end;
        {OrigTypeVal, NewTypeVal} ->
            assert_field_equivalence(OrigTypeVal, NewTypeVal, Name, Key)
    end.

%% Classic queues do not necessarily have an x-queue-type field associated with them
%% so we special-case that scenario here
%%
%% Fixes rabbitmq/rabbitmq-common#341
%%
assert_field_equivalence(Current, Current, _Name, _Key) ->
    ok;
assert_field_equivalence(undefined, {longstr, <<"classic">>}, _Name, <<"x-queue-type">>) ->
    ok;
assert_field_equivalence({longstr, <<"classic">>}, undefined, _Name, <<"x-queue-type">>) ->
    ok;
assert_field_equivalence(Orig, New, Name, Key) ->
    equivalence_fail(Orig, New, Name, Key).

equivalence_fail(Orig, New, Name, Key) ->
    protocol_error(precondition_failed, "inequivalent arg '~ts' "
                   "for ~ts: received ~ts but current is ~ts",
                   [Key, rs(Name), val(New), val(Orig)]).

val(undefined) ->
    "none";
val({Type, Value}) ->
    ValFmt = case is_binary(Value) of
                 true  -> "~ts";
                 false -> "~tp"
             end,
    format("the value '" ++ ValFmt ++ "' of type '~ts'", [Value, Type]);
val(Value) ->
    format(case is_binary(Value) of
               true  -> "'~ts'";
               false -> "'~tp'"
           end, [Value]).

%%
%% Attribute Tables
%%

table_lookup(Table, Key) ->
    case lists:keysearch(Key, 1, Table) of
        {value, {_, Type, Value}} -> {Type, Value};
        false -> undefined
    end.

set_table_value(Table, Key, Type, Value) ->
    sort_field_table(
      lists:keystore(Key, 1, Table, {Key, Type, Value})).

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
      lists:flatten(io_lib:format("~tp.~tp", [Before, After])));
amqp_value(_Type, V) when is_binary(V) -> utf8_safe(V);
amqp_value(_Type, V)                   -> V.


%%
%% Resources
%%

r(#resource{virtual_host = VHostPath}, Kind, Name) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = Name};
r(VHostPath, Kind, Name) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = Name}.

r(VHostPath, Kind) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = '_'}.

r_arg(#resource{virtual_host = VHostPath}, Kind, Table, Key) ->
    r_arg(VHostPath, Kind, Table, Key);
r_arg(VHostPath, Kind, Table, Key) ->
    case table_lookup(Table, Key) of
        {longstr, NameBin} -> r(VHostPath, Kind, NameBin);
        undefined          -> undefined;
        {Type, _}          -> {error, {invalid_type, Type}}
    end.

rs(#resource{virtual_host = VHostPath, kind = topic, name = Name}) ->
    format("'~ts' in vhost '~ts'", [Name, VHostPath]);
rs(#resource{virtual_host = VHostPath, kind = Kind, name = Name}) ->
    format("~ts '~ts' in vhost '~ts'", [Kind, Name, VHostPath]).

-spec queue_resource(rabbit_types:vhost(), resource_name()) ->
    rabbit_types:r(queue).
queue_resource(VHostPath, Name) ->
    r(VHostPath, queue, Name).

-spec exchange_resource(rabbit_types:vhost(), resource_name()) ->
    rabbit_types:r(exchange).
exchange_resource(VHostPath, Name) ->
    r(VHostPath, exchange, Name).

%% @doc Halts the emulator returning the given status code to the os.
%% On Windows this function will block indefinitely so as to give the io
%% subsystem time to flush stdout completely.
quit(Status) ->
    case os:type() of
        {unix,  _} -> halt(Status);
        {win32, _} -> init:stop(Status),
                      receive
                      after infinity -> ok
                      end
    end.

throw_on_error(E, Thunk) ->
    case Thunk() of
        {error, Reason} -> throw({E, Reason});
        {ok, Res}       -> Res;
        Res             -> Res
    end.

with_exit_handler(Handler, Thunk) ->
    try
        Thunk()
    catch
        exit:{R, _}      when ?IS_BENIGN_EXIT(R) -> Handler();
        exit:{{R, _}, _} when ?IS_BENIGN_EXIT(R) -> Handler()
    end.

is_abnormal_exit(R)      when ?IS_BENIGN_EXIT(R) -> false;
is_abnormal_exit({R, _}) when ?IS_BENIGN_EXIT(R) -> false;
is_abnormal_exit(_)                              -> true.

filter_exit_map(F, L) ->
    Ref = make_ref(),
    lists:filter(fun (R) -> R =/= Ref end,
                 [with_exit_handler(
                    fun () -> Ref end,
                    fun () -> F(I) end) || I <- L]).

ensure_ok(ok, _) -> ok;
ensure_ok({error, Reason}, ErrorTag) -> throw({error, {ErrorTag, Reason}}).

tcp_name(Prefix, IPAddress, Port)
  when is_atom(Prefix) andalso is_number(Port) ->
    list_to_atom(
      format("~w_~ts:~w", [Prefix, inet_parse:ntoa(IPAddress), Port])).

format_inet_error(E) -> format("~w (~ts)", [E, format_inet_error0(E)]).

format_inet_error0(address) -> "cannot connect to host/port";
format_inet_error0(timeout) -> "timed out";
format_inet_error0(Error)   -> inet:format_error(Error).

%% base64:decode throws lots of weird errors. Catch and convert to one
%% that will cause a bad_request.
b64decode_or_throw(B64) ->
    try
        base64:decode(B64)
    catch error:_ ->
            throw({error, {not_base64, B64}})
    end.

utf8_safe(V) ->
    try
        _ = xmerl_ucs:from_utf8(V),
        V
    catch exit:{ucs, _} ->
            Enc = split_lines(base64:encode(V)),
            <<"Not UTF-8, base64 is: ", Enc/binary>>
    end.

%% MIME enforces a limit on line length of base 64-encoded data to 76 characters.
split_lines(<<Text:76/binary, Rest/binary>>) ->
    <<Text/binary, $\n, (split_lines(Rest))/binary>>;
split_lines(Text) ->
    Text.


%% This is a modified version of Luke Gorrie's pmap -
%% https://lukego.livejournal.com/6753.html - that doesn't care about
%% the order in which results are received.
%%
%% WARNING: This is is deliberately lightweight rather than robust -- if F
%% throws, upmap will hang forever, so make sure F doesn't throw!
upmap(F, L) ->
    Parent = self(),
    Ref = make_ref(),
    [receive {Ref, Result} -> Result end
     || _ <- [spawn(fun () -> Parent ! {Ref, F(X)} end) || X <- L]].

map_in_order(F, L) ->
    lists:reverse(
      lists:foldl(fun (E, Acc) -> [F(E) | Acc] end, [], L)).

dirty_dump_log(FileName) ->
    {ok, LH} = disk_log:open([{name, dirty_dump_log},
                              {mode, read_only},
                              {file, FileName}]),
    dirty_dump_log1(LH, disk_log:chunk(LH, start)),
    disk_log:close(LH).

dirty_dump_log1(_LH, eof) ->
    io:format("Done.~n");
dirty_dump_log1(LH, {K, Terms}) ->
    io:format("Chunk: ~tp~n", [Terms]),
    dirty_dump_log1(LH, disk_log:chunk(LH, K));
dirty_dump_log1(LH, {K, Terms, BadBytes}) ->
    io:format("Bad Chunk, ~tp: ~tp~n", [BadBytes, Terms]),
    dirty_dump_log1(LH, disk_log:chunk(LH, K)).

format(Fmt, Args) -> lists:flatten(io_lib:format(Fmt, Args)).

format_many(List) ->
    lists:flatten([io_lib:format(F ++ "~n", A) || {F, A} <- List]).

format_stderr(Fmt, Args) ->
    io:format(standard_error, Fmt, Args),
    ok.

unfold(Fun, Init) ->
    unfold(Fun, [], Init).

unfold(Fun, Acc, Init) ->
    case Fun(Init) of
        {true, E, I} -> unfold(Fun, [E|Acc], I);
        false -> {Acc, Init}
    end.

ceil(N) ->
    T = trunc(N),
    case N == T of
        true  -> T;
        false -> 1 + T
    end.

parse_bool(<<"true">>)  -> true;
parse_bool(<<"false">>) -> false;
parse_bool(true)        -> true;
parse_bool(false)       -> false;
parse_bool(undefined)   -> undefined;
parse_bool(V)           -> throw({error, {not_boolean, V}}).

parse_int(I) when is_integer(I) -> I;
parse_int(F) when is_number(F)  -> trunc(F);
parse_int(S)                    -> try
                                       list_to_integer(binary_to_list(S))
                                   catch error:badarg ->
                                           throw({error, {not_integer, S}})
                                   end.


queue_fold(Fun, Init, Q) ->
    case queue:out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> queue_fold(Fun, Fun(V, Init), Q1)
    end.

%% Sorts a list of AMQP 0-9-1 table fields as per the AMQP 0-9-1 spec
sort_field_table([]) ->
    [];
sort_field_table(M) when is_map(M) andalso map_size(M) =:= 0 ->
    [];
sort_field_table(Arguments) when is_map(Arguments) ->
    sort_field_table(maps:to_list(Arguments));
sort_field_table(Arguments) ->
    lists:keysort(1, Arguments).

%% This provides a string representation of a pid that is the same
%% regardless of what node we are running on. The representation also
%% permits easy identification of the pid's node.
pid_to_string(Pid) when is_pid(Pid) ->
    {Node, Cre, Id, Ser} = decompose_pid(Pid),
    format("<~ts.~B.~B.~B>", [Node, Cre, Id, Ser]).

-spec hexify(binary() | atom() | list()) -> binary().
hexify(Bin) when is_binary(Bin) ->
    iolist_to_binary([io_lib:format("~2.16.0B", [V]) || <<V:8>> <= Bin]);
hexify(Bin) when is_list(Bin) ->
    hexify(erlang:list_to_binary(Bin));
hexify(Bin) when is_atom(Bin) ->
    hexify(erlang:atom_to_binary(Bin)).

%% inverse of above
string_to_pid(Str) ->
    Err = {error, {invalid_pid_syntax, Str}},
    %% The \ before the trailing $ is only there to keep emacs
    %% font-lock from getting confused.
    case re:run(Str, "^<(.*)\\.(\\d+)\\.(\\d+)\\.(\\d+)>\$",
                [{capture,all_but_first,list}]) of
        {match, [NodeStr, CreStr, IdStr, SerStr]} ->
            [Cre, Id, Ser] = lists:map(fun list_to_integer/1,
                                       [CreStr, IdStr, SerStr]),
            compose_pid(list_to_atom(NodeStr), Cre, Id, Ser);
        nomatch ->
            throw(Err)
    end.

pid_change_node(Pid, NewNode) ->
    {_OldNode, Cre, Id, Ser} = decompose_pid(Pid),
    compose_pid(NewNode, Cre, Id, Ser).

%% node(node_to_fake_pid(Node)) =:= Node.
node_to_fake_pid(Node) ->
    compose_pid(Node, 0, 0, 0).

decompose_pid(Pid) when is_pid(Pid) ->
    %% see http://erlang.org/doc/apps/erts/erl_ext_dist.html (8.10 and
    %% 8.7)
    Node = node(Pid),
    BinPid0 = term_to_binary(Pid),
    case BinPid0 of
        %% NEW_PID_EXT
        <<131, 88, BinPid/bits>> ->
            NodeByteSize = byte_size(BinPid0) - 14,
            <<_NodePrefix:NodeByteSize/binary, Id:32, Ser:32, Cre:32>> = BinPid,
            {Node, Cre, Id, Ser};
        %% PID_EXT
        <<131, 103, BinPid/bits>> ->
            NodeByteSize = byte_size(BinPid0) - 11,
            <<_NodePrefix:NodeByteSize/binary, Id:32, Ser:32, Cre:8>> = BinPid,
            {Node, Cre, Id, Ser}
    end.

compose_pid(Node, Cre, Id, Ser) ->
    <<131,NodeEnc/binary>> = term_to_binary(Node),
    binary_to_term(<<131,88,NodeEnc/binary,Id:32,Ser:32,Cre:32>>).

version_compare(A, B, eq)  -> rabbit_semver:eql(A, B);
version_compare(A, B, lt)  -> rabbit_semver:lt(A, B);
version_compare(A, B, lte) -> rabbit_semver:lte(A, B);
version_compare(A, B, gt)  -> rabbit_semver:gt(A, B);
version_compare(A, B, gte) -> rabbit_semver:gte(A, B).

version_compare(A, B) ->
    case version_compare(A, B, lt) of
        true -> lt;
        false -> case version_compare(A, B, gt) of
                     true -> gt;
                     false -> eq
                 end
    end.

%% The function below considers that e.g. 3.7.x and 3.8.x are incompatible (as
%% if there were no feature flags). This is useful to check plugin
%% compatibility (`broker_versions_requirement` field in plugins).

strict_version_minor_equivalent(A, B) ->
    {{MajA, MinA, _PatchA, _}, _} = rabbit_semver:normalize(rabbit_semver:parse(A)),
    {{MajB, MinB, _PatchB, _}, _} = rabbit_semver:normalize(rabbit_semver:parse(B)),

    MajA =:= MajB andalso MinA =:= MinB.

dict_cons(Key, Value, Dict) ->
    dict:update(Key, fun (List) -> [Value | List] end, [Value], Dict).

orddict_cons(Key, Value, Dict) ->
    orddict:update(Key, fun (List) -> [Value | List] end, [Value], Dict).

maps_cons(Key, Value, Map) ->
    maps:update_with(Key, fun (List) -> [Value | List] end, [Value], Map).

gb_trees_cons(Key, Value, Tree) ->
    case gb_trees:lookup(Key, Tree) of
        {value, Values} -> gb_trees:update(Key, [Value | Values], Tree);
        none            -> gb_trees:insert(Key, [Value], Tree)
    end.

gb_trees_fold(Fun, Acc, Tree) ->
    gb_trees_fold1(Fun, Acc, gb_trees:next(gb_trees:iterator(Tree))).

gb_trees_fold1(_Fun, Acc, none) ->
    Acc;
gb_trees_fold1(Fun, Acc, {Key, Val, It}) ->
    gb_trees_fold1(Fun, Fun(Key, Val, Acc), gb_trees:next(It)).

gb_trees_foreach(Fun, Tree) ->
    gb_trees_fold(fun (Key, Val, Acc) -> Fun(Key, Val), Acc end, ok, Tree).

module_attributes(Module) ->
    try
        Module:module_info(attributes)
    catch
        _:undef ->
            io:format("WARNING: module ~tp not found, so not scanned for boot steps.~n",
                      [Module]),
            []
    end.

all_module_attributes(Name) ->
    Apps = [App || {App, _, _} <- application:loaded_applications()],
    module_attributes_from_apps(Name, Apps).

rabbitmq_related_module_attributes(Name) ->
    Apps = rabbitmq_related_apps(),
    module_attributes_from_apps(Name, Apps).

rabbitmq_related_apps() ->
    [App
     || {App, _, _} <- application:loaded_applications(),
        %% Only select RabbitMQ-related applications.
        App =:= rabbit_common orelse
        App =:= rabbitmq_prelaunch orelse
        App =:= rabbit orelse
        lists:member(
          rabbit,
          element(2, application:get_key(App, applications)))].

module_attributes_from_apps(Name, Apps) ->
    Targets =
        lists:usort(
          lists:append(
            [[{App, Module} || Module <- Modules] ||
                App           <- Apps,
                {ok, Modules} <- [application:get_key(App, modules)]])),
    lists:foldl(
      fun ({App, Module}, Acc) ->
              case lists:append([Atts || {N, Atts} <- module_attributes(Module),
                                         N =:= Name]) of
                  []   -> Acc;
                  Atts -> [{App, Module, Atts} | Acc]
              end
      end, [], Targets).

build_acyclic_graph(VertexFun, EdgeFun, Graph) ->
    G = digraph:new([acyclic]),
    try
        _ = [case digraph:vertex(G, Vertex) of
                 false -> digraph:add_vertex(G, Vertex, Label);
                 _     -> ok = throw({graph_error, {vertex, duplicate, Vertex}})
             end || GraphElem       <- Graph,
                    {Vertex, Label} <- VertexFun(GraphElem)],
        [case digraph:add_edge(G, From, To) of
             {error, E} -> throw({graph_error, {edge, E, From, To}});
             _          -> ok
         end || GraphElem  <- Graph,
                {From, To} <- EdgeFun(GraphElem)],
        {ok, G}
    catch {graph_error, Reason} ->
            true = digraph:delete(G),
            {error, Reason}
    end.

const(X) -> fun () -> X end.

%% Format IPv4-mapped IPv6 addresses as IPv4, since they're what we see
%% when IPv6 is enabled but not used (i.e. 99% of the time).
ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

ntoab(IP) ->
    Str = ntoa(IP),
    case string:str(Str, ":") of
        0 -> Str;
        _ -> "[" ++ Str ++ "]"
    end.

%% We try to avoid reconnecting to down nodes here; this is used in a
%% loop in rabbit_amqqueue:on_node_down/1 and any delays we incur
%% would be bad news.
%%
%% See also rabbit_process:is_process_alive/1 which also requires the
%% process be in the same running cluster as us (i.e. not partitioned
%% or some random node).
is_process_alive(Pid) when node(Pid) =:= node() ->
    erlang:is_process_alive(Pid);
is_process_alive(Pid) ->
    Node = node(Pid),
    lists:member(Node, [node() | nodes(connected)]) andalso
        rpc:call(Node, erlang, is_process_alive, [Pid]) =:= true.

%% Get process info of a prossibly remote process.
%% We try to avoid reconnecting to down nodes.
-spec process_info(pid(), ItemSpec) -> Result| undefined | {badrpc, term()}
              when
      ItemSpec :: atom() | list() | tuple(),
      Result :: {atom() | tuple(), term()} | [{atom() | tuple(), term()}].
process_info(Pid, Items) when node(Pid) =:= node() ->
    erlang:process_info(Pid, Items);
process_info(Pid, Items) ->
    Node = node(Pid),
    case lists:member(Node, [node() | nodes(connected)]) of
        true ->
            rpc:call(Node, erlang, process_info, [Pid, Items]);
        _ ->
            {badrpc, nodedown}
    end.

-spec pget(term(), list() | map()) -> term().
pget(K, M) when is_map(M) ->
    maps:get(K, M, undefined);

pget(K, P) ->
    case lists:keyfind(K, 1, P) of
        {K, V} ->
            V;
        _ ->
            undefined
    end.

-spec pget(term(), list() | map(), term()) -> term().
pget(K, M, D) when is_map(M) ->
    maps:get(K, M, D);

pget(K, P, D) ->
    case lists:keyfind(K, 1, P) of
        {K, V} ->
            V;
        _ ->
            D
    end.

-spec pget_or_die(term(), list() | map()) -> term() | no_return().
pget_or_die(K, M) when is_map(M) ->
    case maps:find(K, M) of
        error   -> exit({error, key_missing, K});
        {ok, V} -> V
    end;

pget_or_die(K, P) ->
    case proplists:get_value(K, P) of
        undefined -> exit({error, key_missing, K});
        V         -> V
    end.

pupdate(K, UpdateFun, P) ->
    case lists:keyfind(K, 1, P) of
        {K, V} ->
            pset(K, UpdateFun(V), P);
        _ ->
            undefined
    end.

%% pget nested values
-spec deep_pget(list(), list() | map()) -> term().
deep_pget(K, P) ->
    deep_pget(K, P, undefined).

-spec deep_pget(list(), list() | map(), term()) -> term().
deep_pget([], P, _) ->
    P;

deep_pget([K|Ks], P, D) ->
    case rabbit_misc:pget(K, P, D) of
        D -> D;
        Pn -> deep_pget(Ks, Pn, D)
    end.

%% property merge
pmerge(Key, Val, List) ->
      case proplists:is_defined(Key, List) of
              true -> List;
              _    -> [{Key, Val} | List]
      end.

%% proplists merge
plmerge(P1, P2) ->
    %% Value from P2 supersedes value from P1
    lists:sort(maps:to_list(maps:merge(maps:from_list(P1),
                                       maps:from_list(P2)))).

%% groups a list of proplists by a key function
group_proplists_by(KeyFun, ListOfPropLists) ->
    Res = lists:foldl(fun(P, Agg) ->
                        Key = KeyFun(P),
                        Val = case maps:find(Key, Agg) of
                            {ok, O} -> [P|O];
                            error   -> [P]
                        end,
                        maps:put(Key, Val, Agg)
                      end, #{}, ListOfPropLists),
    [ X || {_, X} <- maps:to_list(Res)].

pset(Key, Value, List) -> [{Key, Value} | proplists:delete(Key, List)].

format_message_queue(_Opt, MQ) ->
    Len = priority_queue:len(MQ),
    {Len,
     case Len > 100 of
         false -> priority_queue:to_list(MQ);
         true  -> {summary,
                   maps:to_list(
                     lists:foldl(
                       fun ({P, V}, Counts) ->
                               maps:update_with(
                                 {P, format_message_queue_entry(V)},
                                 fun(Old) -> Old + 1 end, 1, Counts)
                       end, maps:new(), priority_queue:to_list(MQ)))}
     end}.

format_message_queue_entry(V) when is_atom(V) ->
    V;
format_message_queue_entry(V) when is_tuple(V) ->
    list_to_tuple([format_message_queue_entry(E) || E <- tuple_to_list(V)]);
format_message_queue_entry(_V) ->
    '_'.

%% Same as rpc:multicall/4 but concatenates all results.
%% M, F, A is expected to return a list. If it does not,
%% its return value will be wrapped in a list.
-spec append_rpc_all_nodes([node()], atom(), atom(), [any()]) -> [any()].
append_rpc_all_nodes(Nodes, M, F, A) ->
    do_append_rpc_all_nodes(Nodes, M, F, A, ?RPC_INFINITE_TIMEOUT).

-spec append_rpc_all_nodes([node()], atom(), atom(), [any()], timeout()) -> [any()].
append_rpc_all_nodes(Nodes, M, F, A, Timeout) ->
    do_append_rpc_all_nodes(Nodes, M, F, A, Timeout).

do_append_rpc_all_nodes(Nodes, M, F, A, ?RPC_INFINITE_TIMEOUT) ->
    {ResL, _} = rpc:multicall(Nodes, M, F, A, ?RPC_INFINITE_TIMEOUT),
    process_rpc_multicall_result(ResL);
do_append_rpc_all_nodes(Nodes, M, F, A, Timeout) ->
    {ResL, _} = try
                    rpc:multicall(Nodes, M, F, A, Timeout)
                catch
                    error:internal_error -> {[], Nodes}
                end,
    process_rpc_multicall_result(ResL).

process_rpc_multicall_result(ResL) ->
    lists:append([case Res of
                      {badrpc, _}         -> [];
                      Xs when is_list(Xs) -> Xs;
                      %% wrap it in a list
                      Other               -> [Other]
                  end || Res <- ResL]).

os_cmd(Command) ->
    case os:type() of
        {win32, _} ->
            %% Clink workaround; see
            %% https://code.google.com/p/clink/issues/detail?id=141
            os:cmd(" " ++ Command);
        _ ->
            %% Don't just return "/bin/sh: <cmd>: not found" if not found
            Exec = hd(string:tokens(Command, " ")),
            case os:find_executable(Exec) of
                false -> throw({command_not_found, Exec});
                _     -> os:cmd(Command)
            end
    end.

pwsh_cmd(Command) ->
    case os:type() of
        {win32, _} ->
            do_pwsh_cmd(Command);
        _ ->
            {error, invalid_os_type}
    end.

is_os_process_alive(Pid) ->
    with_os([{unix, fun () ->
                            run_ps(Pid) =:= 0
                    end},
             {win32, fun () ->
                             PidS = rabbit_data_coercion:to_list(Pid),
                             case os:find_executable("tasklist.exe") of
                                 false ->
                                     Cmd = format("(Get-Process -Id ~ts).ProcessName", [PidS]),
                                     {ok, [Res]} = pwsh_cmd(Cmd),
                                     case Res of
                                         "erl"  -> true;
                                         "werl" -> true;
                                         _      -> false
                                     end;
                                 TasklistExe ->
                                     Args = ["/nh", "/fi", "pid eq " ++ PidS],
                                     {ok, [Res]} = win32_cmd(TasklistExe, Args),
                                     match =:= re:run(Res, "erl\\.exe", [{capture, none}])
                             end
                     end}]).

with_os(Handlers) ->
    {OsFamily, _} = os:type(),
    case proplists:get_value(OsFamily, Handlers) of
        undefined -> throw({unsupported_os, OsFamily});
        Handler   -> Handler()
    end.

run_ps(Pid) ->
    Cmd  = "ps -p " ++ rabbit_data_coercion:to_list(Pid),
    Port = erlang:open_port({spawn, Cmd},
                            [exit_status, {line, 16384},
                             use_stdio, stderr_to_stdout]),
    exit_loop(Port).

exit_loop(Port) ->
    receive
        {Port, {exit_status, Rc}} -> Rc;
        {Port, _}                 -> exit_loop(Port)
    end.

version() ->
    {ok, VSN} = application:get_key(rabbit, vsn),
    VSN.

%% See https://www.erlang.org/doc/system_principles/versions.html
otp_release() ->
    File = filename:join([code:root_dir(), "releases",
                          erlang:system_info(otp_release), "OTP_VERSION"]),
    case raw_read_file(File) of
        {ok, VerBin} ->
            %% 17.0 or later, we need the file for the minor version
            string:strip(binary_to_list(VerBin), both, $\n);
        {error, _} ->
            %% R16B03 or earlier (no file, otp_release is correct)
            %% or we couldn't read the file (so this is best we can do)
            erlang:system_info(otp_release)
    end.

platform_and_version() ->
    string:join(["Erlang/OTP", otp_release()], " ").

otp_system_version() ->
    string:strip(erlang:system_info(system_version), both, $\n).

rabbitmq_and_erlang_versions() ->
  {version(), otp_release()}.

%% application:which_applications(infinity) is dangerous, since it can
%% cause deadlocks on shutdown. So we have to use a timeout variant,
%% but w/o creating spurious timeout errors. The timeout value is twice
%% that of gen_server:call/2.
which_applications() ->
    try
        application:which_applications(10000)
    catch _:_:_Stacktrace ->
        []
    end.

sequence_error([T])                      -> T;
sequence_error([{error, _} = Error | _]) -> Error;
sequence_error([_ | Rest])               -> sequence_error(Rest).

check_expiry(N)
  when N < 0 ->
    {error, {value_negative, N}};
check_expiry(N)
  when N > 315_360_000_000 -> %% 10 years in milliseconds
    {error, {value_too_large, N}};
check_expiry(_N) ->
    ok.

base64url(In) ->
    lists:reverse(lists:foldl(fun ($\+, Acc) -> [$\- | Acc];
                                  ($\/, Acc) -> [$\_ | Acc];
                                  ($\=, Acc) -> Acc;
                                  (Chr, Acc) -> [Chr | Acc]
                              end, [], base64:encode_to_string(In))).

%% Ideally, you'd want Fun to run every IdealInterval. but you don't
%% want it to take more than MaxRatio of IdealInterval. So if it takes
%% more then you want to run it less often. So we time how long it
%% takes to run, and then suggest how long you should wait before
%% running it again with a user specified max interval. Times are in millis.
interval_operation({M, F, A}, MaxRatio, MaxInterval, IdealInterval, LastInterval) ->
    {Micros, Res} = timer:tc(M, F, A),
    {Res, case {Micros > 1000 * (MaxRatio * IdealInterval),
                Micros > 1000 * (MaxRatio * LastInterval)} of
              {true,  true}  -> lists:min([MaxInterval,
                                           round(LastInterval * 1.5)]);
              {true,  false} -> LastInterval;
              {false, false} -> lists:max([IdealInterval,
                                           round(LastInterval / 1.5)])
          end}.

ensure_timer(State, Idx, After, Msg) ->
    case element(Idx, State) of
        undefined -> TRef = send_after(After, self(), Msg),
                     setelement(Idx, State, TRef);
        _         -> State
    end.

stop_timer(State, Idx) ->
    case element(Idx, State) of
        undefined -> State;
        TRef      -> cancel_timer(TRef),
                     setelement(Idx, State, undefined)
    end.

%% timer:send_after/3 goes through a single timer process but allows
%% long delays. erlang:send_after/3 does not have a bottleneck but
%% only allows max 2^32-1 millis.
-define(MAX_ERLANG_SEND_AFTER, 4294967295).
send_after(Millis, Pid, Msg) when Millis > ?MAX_ERLANG_SEND_AFTER ->
    {ok, Ref} = timer:send_after(Millis, Pid, Msg),
    {timer, Ref};
send_after(Millis, Pid, Msg) ->
    {erlang, erlang:send_after(Millis, Pid, Msg)}.

cancel_timer({erlang, Ref}) -> _ = erlang:cancel_timer(Ref),
                               ok;
cancel_timer({timer, Ref})  -> {ok, cancel} = timer:cancel(Ref),
                               ok.

store_proc_name(Type, ProcName) -> store_proc_name({Type, ProcName}).
store_proc_name(TypeProcName)   -> put(process_name, TypeProcName).

get_proc_name() ->
    case get(process_name) of
        undefined ->
            undefined;
        {_Type, Name} ->
            {ok, Name}
    end.

%% application:get_env/3 is available in R16B01 or later.
get_env(Application, Key, Def) ->
    application:get_env(Application, Key, Def).

get_channel_operation_timeout() ->
    %% Default channel_operation_timeout set to net_ticktime + 10s to
    %% give allowance for any down messages to be received first,
    %% whenever it is used for cross-node calls with timeouts.
    Default = (net_kernel:get_net_ticktime() + 10) * 1000,
    application:get_env(rabbit, channel_operation_timeout, Default).

moving_average(_Time, _HalfLife, Next, undefined) ->
    Next;
%% We want the Weight to decrease as Time goes up (since Weight is the
%% weight for the current sample, not the new one), so that the moving
%% average decays at the same speed regardless of how long the time is
%% between samplings. So we want Weight = math:exp(Something), where
%% Something turns out to be negative.
%%
%% We want to determine Something here in terms of the Time taken
%% since the last measurement, and a HalfLife. So we want Weight =
%% math:exp(Time * Constant / HalfLife). What should Constant be? We
%% want Weight to be 0.5 when Time = HalfLife.
%%
%% Plug those numbers in and you get 0.5 = math:exp(Constant). Take
%% the log of each side and you get math:log(0.5) = Constant.
moving_average(Time,  HalfLife,  Next, Current) ->
    Weight = math:exp(Time * math:log(0.5) / HalfLife),
    Next * (1 - Weight) + Current * Weight.

random(N) ->
    rand:uniform(N).

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

%% If the server we are talking to has non-standard net_ticktime, and
%% our connection lasts a while, we could get disconnected because of
%% a timeout unless we set our ticktime to be the same. So let's do
%% that.
%% TODO: do not use an infinite timeout!
-spec rpc_call(node(), atom(), atom(), [any()]) -> any() | {badrpc, term()}.
rpc_call(Node, Mod, Fun, Args) ->
    rpc_call(Node, Mod, Fun, Args, ?RPC_INFINITE_TIMEOUT).

-spec rpc_call(node(), atom(), atom(), [any()], infinity | non_neg_integer()) -> any() | {badrpc, term()}.
rpc_call(Node, Mod, Fun, Args, Timeout) ->
    case rpc:call(Node, net_kernel, get_net_ticktime, [], Timeout) of
        {badrpc, _} = E -> E;
        ignored ->
            rpc:call(Node, Mod, Fun, Args, Timeout);
        {ongoing_change_to, NewValue} ->
            _ = net_kernel:set_net_ticktime(NewValue, 0),
            rpc:call(Node, Mod, Fun, Args, Timeout);
        Time            ->
            _ = net_kernel:set_net_ticktime(Time, 0),
            rpc:call(Node, Mod, Fun, Args, Timeout)
    end.

get_gc_info(Pid) ->
    rabbit_runtime:get_gc_info(Pid).

-spec raw_read_file(Filename) -> {ok, Binary} | {error, Reason} when
      Filename :: file:name_all(),
      Binary :: binary(),
      Reason :: file:posix() | badarg | terminated | system_limit.
raw_read_file(File) ->
    try
        % Note: this works around the win32 file leak in file:read_file/1
        % https://github.com/erlang/otp/issues/5527
        {ok, FInfo} = file:read_file_info(File, [raw]),
        {ok, Fd} = file:open(File, [read, raw, binary]),
        try
            file:read(Fd, FInfo#file_info.size)
        after
            file:close(Fd)
        end
    catch
        error:{badmatch, Error} -> Error
    end.

-spec is_regular_file(Name) -> boolean() when
      Name :: file:filename_all().
is_regular_file(Name) ->
    % Note: this works around the win32 file leak in file:read_file/1
    % https://github.com/erlang/otp/issues/5527
    case file:read_file_info(Name, [raw]) of
        {ok, #file_info{type=regular}} -> true;
        _ -> false
    end.

-spec safe_ets_update_counter(Table, Key, UpdateOp) -> Result when
      Table :: ets:table(),
      Key :: term(),
      UpdateOp :: {Pos, Incr}
      | {Pos, Incr, Threshold, SetValue},
      Pos :: integer(),
      Incr :: integer(),
      Threshold :: integer(),
      SetValue :: integer(),
      Result :: integer();
    (Table, Key, [UpdateOp]) -> [Result] when
      Table :: ets:table(),
      Key :: term(),
      UpdateOp :: {Pos, Incr}
      | {Pos, Incr, Threshold, SetValue},
      Pos :: integer(),
      Incr :: integer(),
      Threshold :: integer(),
      SetValue :: integer(),
      Result :: integer();
    (Table, Key, Incr) -> Result when
      Table :: ets:table(),
      Key :: term(),
      Incr :: integer(),
      Result :: integer().
safe_ets_update_counter(Tab, Key, UpdateOp) ->
  try
    ets:update_counter(Tab, Key, UpdateOp)
  catch error:badarg:E ->
    rabbit_log:debug("error updating ets counter ~p in table ~p: ~p", [Key, Tab, E]),
    ok
  end.

-spec safe_ets_update_counter(Table, Key, UpdateOp, OnFailure) -> Result when
    Table :: ets:table(),
    Key :: term(),
    UpdateOp :: {Pos, Incr}
    | {Pos, Incr, Threshold, SetValue},
    Pos :: integer(),
    Incr :: integer(),
    Threshold :: integer(),
    SetValue :: integer(),
    Result :: integer(),
    OnFailure :: fun(() -> any());
  (Table, Key, [UpdateOp], OnFailure) -> [Result] when
    Table :: ets:table(),
    Key :: term(),
    UpdateOp :: {Pos, Incr}
    | {Pos, Incr, Threshold, SetValue},
    Pos :: integer(),
    Incr :: integer(),
    Threshold :: integer(),
    SetValue :: integer(),
    Result :: integer(),
    OnFailure :: fun(() -> any());
  (Table, Key, Incr, OnFailure) -> Result when
    Table :: ets:table(),
    Key :: term(),
    Incr :: integer(),
    Result :: integer(),
    OnFailure :: fun(() -> any()).
safe_ets_update_counter(Tab, Key, UpdateOp, OnFailure) ->
  safe_ets_update_counter(Tab, Key, UpdateOp, fun(_) -> ok end, OnFailure).

-spec safe_ets_update_counter(Table, Key, UpdateOp, OnSuccess, OnFailure) -> Result when
    Table :: ets:table(),
    Key :: term(),
    UpdateOp :: {Pos, Incr}
    | {Pos, Incr, Threshold, SetValue},
    Pos :: integer(),
    Incr :: integer(),
    Threshold :: integer(),
    SetValue :: integer(),
    Result :: integer(),
    OnSuccess :: fun((boolean()) -> any()),
    OnFailure :: fun(() -> any());
  (Table, Key, [UpdateOp], OnSuccess, OnFailure) -> [Result] when
    Table :: ets:table(),
    Key :: term(),
    UpdateOp :: {Pos, Incr}
    | {Pos, Incr, Threshold, SetValue},
    Pos :: integer(),
    Incr :: integer(),
    Threshold :: integer(),
    SetValue :: integer(),
    Result :: integer(),
    OnSuccess :: fun((boolean()) -> any()),
    OnFailure :: fun(() -> any());
  (Table, Key, Incr, OnSuccess, OnFailure) -> Result when
  Table :: ets:table(),
  Key :: term(),
  Incr :: integer(),
  Result :: integer(),
  OnSuccess :: fun((integer()) -> any()),
  OnFailure :: fun(() -> any()).
safe_ets_update_counter(Tab, Key, UpdateOp, OnSuccess, OnFailure) ->
  try
    OnSuccess(ets:update_counter(Tab, Key, UpdateOp))
  catch error:badarg:E ->
    rabbit_log:debug("error updating ets counter ~p in table ~p: ~p", [Key, Tab, E]),
    OnFailure()
  end.


-spec safe_ets_update_element(Table, Key, ElementSpec :: {Pos, Value}) -> boolean() when
    Table :: ets:table(),
    Key :: term(),
    Pos :: pos_integer(),
    Value :: term();
  (Table, Key, ElementSpec :: [{Pos, Value}]) -> boolean() when
    Table :: ets:table(),
    Key :: term(),
    Pos :: pos_integer(),
    Value :: term().
safe_ets_update_element(Tab, Key, ElementSpec) ->
  try
    ets:update_element(Tab, Key, ElementSpec)
  catch error:badarg:E ->
    rabbit_log:debug("error updating ets element ~p in table ~p: ~p", [Key, Tab, E]),
    false
  end.

-spec safe_ets_update_element(Table, Key, ElementSpec :: {Pos, Value}, OnFailure) -> boolean() when
    Table :: ets:table(),
    Key :: term(),
    Pos :: pos_integer(),
    Value :: term(),
    OnFailure :: fun(() -> any());
  (Table, Key, ElementSpec :: [{Pos, Value}], OnFailure) -> boolean() when
    Table :: ets:table(),
    Key :: term(),
    Pos :: pos_integer(),
    Value :: term(),
    OnFailure :: fun(() -> any()).
safe_ets_update_element(Tab, Key, ElementSpec, OnFailure) ->
  safe_ets_update_element(Tab, Key, ElementSpec, fun(_) -> ok end, OnFailure).

-spec safe_ets_update_element(Table, Key, ElementSpec :: {Pos, Value}, OnSuccess, OnFailure) -> boolean() when
    Table :: ets:table(),
    Key :: term(),
    Pos :: pos_integer(),
    Value :: term(),
    OnSuccess :: fun((boolean()) -> any()),
    OnFailure :: fun(() -> any());
  (Table, Key, ElementSpec :: [{Pos, Value}], OnSuccess, OnFailure) -> boolean() when
    Table :: ets:table(),
    Key :: term(),
    Pos :: pos_integer(),
    Value :: term(),
    OnSuccess :: fun((boolean()) -> any()),
    OnFailure :: fun(() -> any()).
safe_ets_update_element(Tab, Key, ElementSpec, OnSuccess, OnFailure) ->
  try
    OnSuccess(ets:update_element(Tab, Key, ElementSpec))
  catch error:badarg:E ->
    rabbit_log:debug("error updating ets element ~p in table ~p: ~p", [Key, Tab, E]),
    OnFailure(),
    false
  end.

%% this used to be in supervisor2
-spec find_child(Supervisor, Name) -> [pid()] when
      Supervisor :: rabbit_types:sup_ref(),
      Name :: rabbit_types:child_id().
find_child(Supervisor, Name) ->
    [Pid || {Name1, Pid, _Type, _Modules} <- supervisor:which_children(Supervisor),
            Name1 =:= Name].

%% -------------------------------------------------------------------------
%% Begin copypasta from gen_server2.erl

get_parent() ->
    case get('$ancestors') of
        [Parent | _] when is_pid (Parent) -> Parent;
        [Parent | _] when is_atom(Parent) -> name_to_pid(Parent);
        _ -> exit(process_was_not_started_by_proc_lib)
    end.

name_to_pid(Name) ->
    case whereis(Name) of
        undefined -> case whereis_name(Name) of
                         undefined -> exit(could_not_find_registered_name);
                         Pid       -> Pid
                     end;
        Pid       -> Pid
    end.

whereis_name(Name) ->
    case ets:lookup(global_names, Name) of
        [{_Name, Pid, _Method, _RPid, _Ref}] ->
            if node(Pid) == node() -> case erlang:is_process_alive(Pid) of
                                          true  -> Pid;
                                          false -> undefined
                                      end;
               true                -> Pid
            end;
        [] -> undefined
    end.

%% End copypasta from gen_server2.erl
%% -------------------------------------------------------------------------
%% This will execute a Powershell command without an intervening cmd.exe
%% process. Output lines can't exceed 512 bytes.
%%
%% Inspired by os:cmd/1 in lib/kernel/src/os.erl
do_pwsh_cmd(Command) ->
    Pwsh = find_powershell(),
    Args = ["-NoLogo",
            "-NonInteractive",
            "-NoProfile",
            "-InputFormat", "Text",
            "-OutputFormat", "Text",
            "-Command", Command],
    win32_cmd(Pwsh, Args).

win32_cmd(Exe, Args) ->
    SystemRootDir = os:getenv("SystemRoot", "/"),
    % Note: 'hide' must be used or this will not work!
    A0 = [exit_status, stderr_to_stdout, in, hide,
          {cd, SystemRootDir}, {line, 512}, {arg0, Exe}, {args, Args}],
    Port = erlang:open_port({spawn_executable, Exe}, A0),
    MonRef = erlang:monitor(port, Port),
    Result = win32_cmd_receive(Port, MonRef, []),
    true = erlang:demonitor(MonRef, [flush]),
    Result.

win32_cmd_receive(Port, MonRef, Acc0) ->
    receive
        {Port, {exit_status, 0}} ->
            win32_cmd_receive_finish(Port, MonRef),
            {ok, lists:reverse(Acc0)};
        {Port, {exit_status, Status}} ->
            win32_cmd_receive_finish(Port, MonRef),
            {error, {exit_status, Status}};
        {Port, {data, {eol, Data0}}} ->
            Data1 = string:trim(Data0),
            Acc1 = case Data1 of
                       [] -> Acc0; % Note: skip empty lines in output
                       Data2 -> [Data2 | Acc0]
                   end,
            win32_cmd_receive(Port, MonRef, Acc1);
        {'DOWN', MonRef, _, _, _} ->
            flush_exit(Port),
            {error, nodata}
    after 5000 ->
              win32_cmd_receive_finish(Port, MonRef),
              {error, timeout}
    end.

win32_cmd_receive_finish(Port, MonRef) ->
    catch erlang:port_close(Port),
    flush_until_down(Port, MonRef).

flush_until_down(Port, MonRef) ->
    receive
        {Port, {data, _Bytes}} ->
            flush_until_down(Port, MonRef);
        {'DOWN', MonRef, _, _, _} ->
            flush_exit(Port)
    after 500 ->
              flush_exit(Port)
    end.

flush_exit(Port) ->
    receive
        {'EXIT', Port, _} -> ok
    after 0 ->
              ok
    end.

find_powershell() ->
    case os:find_executable("pwsh.exe") of
        false ->
            case os:find_executable("powershell.exe") of
                false ->
                    "powershell.exe";
                PowershellExe ->
                    PowershellExe
            end;
        PwshExe ->
            PwshExe
    end.

%% Returns true if Pred(Key, Value) returns true for at least one Key to Value association in Map.
%% The Pred function must return a boolean.
-spec maps_any(Pred, Map) -> boolean() when
      Pred :: fun((Key, Value) -> boolean()),
      Map :: #{Key => Value}.
maps_any(Pred, Map)
  when is_function(Pred, 2) andalso is_map(Map) ->
    I = maps:iterator(Map),
    maps_any_1(Pred, maps:next(I)).

maps_any_1(_Pred, none) ->
    false;
maps_any_1(Pred, {K, V, I}) ->
    case Pred(K, V) of
        true ->
            true;
        false ->
            maps_any_1(Pred, maps:next(I))
    end.

-spec is_even(integer()) -> boolean().
is_even(N) ->
    (N band 1) =:= 0.
-spec is_odd(integer()) -> boolean().
is_odd(N) ->
    (N band 1) =:= 1.

-spec maps_put_truthy(Key, Value, Map) -> Map when
      Map :: #{Key => Value}.
maps_put_truthy(_K, undefined, M) ->
    M;
maps_put_truthy(_K, false, M) ->
    M;
maps_put_truthy(K, V, M) ->
    maps:put(K, V, M).

-spec maps_put_falsy(Key, Value, Map) -> Map when
      Map :: #{Key => Value}.
maps_put_falsy(K, undefined, M) ->
    maps:put(K, undefined, M);
maps_put_falsy(K, false, M) ->
    maps:put(K, false, M);
maps_put_falsy(_K, _V, M) ->
    M.

-spec remote_sup_child(node(), rabbit_types:sup_ref()) -> rabbit_types:ok_or_error2(rabbit_types:child(), no_child | no_sup).
remote_sup_child(Node, Sup) ->
    case rpc:call(Node, supervisor, which_children, [Sup]) of
        [{_, Child, _, _}]              -> {ok, Child};
        []                              -> {error, no_child};
        {badrpc, {'EXIT', {noproc, _}}} -> {error, no_sup}
    end.

-spec for_each_while_ok(ForEachFun, List) -> Ret when
      ForEachFun :: fun((Element) -> ok | {error, ErrReason}),
      ErrReason :: any(),
      Element :: any(),
      List :: [Element],
      Ret :: ok | {error, ErrReason}.
%% @doc Calls the given `ForEachFun' for each element in the given `List',
%% short-circuiting if the function returns `{error,_}'.
%%
%% @returns the first `{error,_}' returned by `ForEachFun' or `ok' if
%% `ForEachFun' never returns an error tuple.

for_each_while_ok(Fun, [Elem | Rest]) ->
    case Fun(Elem) of
        ok ->
            for_each_while_ok(Fun, Rest);
        {error, _} = Error ->
            Error
    end;
for_each_while_ok(_, []) ->
    ok.

-spec fold_while_ok(FoldFun, Acc, List) -> Ret when
      FoldFun :: fun((Element, Acc) -> {ok, Acc} | {error, ErrReason}),
      Element :: any(),
      List :: Element,
      Ret :: {ok, Acc} | {error, ErrReason}.
%% @doc Calls the given `FoldFun' on each element of the given `List' and the
%% accumulator value, short-circuiting if the function returns `{error,_}'.
%%
%% @returns the first `{error,_}' returned by `FoldFun' or `{ok,Acc}' if
%% `FoldFun' never returns an error tuple.

fold_while_ok(Fun, Acc0, [Elem | Rest]) ->
    case Fun(Elem, Acc0) of
        {ok, Acc} ->
            fold_while_ok(Fun, Acc, Rest);
        {error, _} = Error ->
            Error
    end;
fold_while_ok(_Fun, Acc, []) ->
    {ok, Acc}.
