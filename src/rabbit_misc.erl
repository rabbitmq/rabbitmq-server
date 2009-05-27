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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_misc).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("kernel/include/file.hrl").

-export([method_record_type/1, polite_pause/0, polite_pause/1]).
-export([die/1, frame_error/2, protocol_error/3, protocol_error/4]).
-export([not_found/1]).
-export([get_config/1, get_config/2, set_config/2]).
-export([dirty_read/1]).
-export([r/3, r/2, r_arg/4, rs/1]).
-export([enable_cover/0, report_cover/0]).
-export([throw_on_error/2, with_exit_handler/2, filter_exit_map/2]).
-export([with_user/2, with_vhost/2, with_user_and_vhost/3]).
-export([execute_mnesia_transaction/1]).
-export([ensure_ok/2]).
-export([localnode/1, tcp_name/3]).
-export([intersperse/2, upmap/2, map_in_order/2]).
-export([table_foreach/2]).
-export([dirty_read_all/1, dirty_foreach_key/2, dirty_dump_log/1]).
-export([append_file/2, ensure_parent_dirs_exist/1]).
-export([format_stderr/2]).
-export([start_applications/1, stop_applications/1]).

-import(mnesia).
-import(lists).
-import(cover).
-import(disk_log).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-include_lib("kernel/include/inet.hrl").

-spec(method_record_type/1 :: (tuple()) -> atom()).
-spec(polite_pause/0 :: () -> 'done').
-spec(polite_pause/1 :: (non_neg_integer()) -> 'done').
-spec(die/1 :: (atom()) -> no_return()).
-spec(frame_error/2 :: (atom(), binary()) -> no_return()).
-spec(protocol_error/3 ::
      (atom() | amqp_error(), string(), [any()]) -> no_return()).
-spec(protocol_error/4 ::
      (atom() | amqp_error(), string(), [any()], atom()) -> no_return()).
-spec(not_found/1 :: (r(atom())) -> no_return()).
-spec(get_config/1 :: (atom()) -> {'ok', any()} | not_found()).
-spec(get_config/2 :: (atom(), A) -> A).
-spec(set_config/2 :: (atom(), any()) -> 'ok').
-spec(dirty_read/1 :: ({atom(), any()}) -> {'ok', any()} | not_found()).
-spec(r/3 :: (vhost() | r(atom()), K, resource_name()) ->
             r(K) when is_subtype(K, atom())).
-spec(r/2 :: (vhost(), K) -> #resource{virtual_host :: vhost(),
                                       kind         :: K,
                                       name         :: '_'}
                                 when is_subtype(K, atom())).
-spec(r_arg/4 :: (vhost() | r(atom()), K, amqp_table(), binary()) ->
             undefined | r(K)  when is_subtype(K, atom())).
-spec(rs/1 :: (r(atom())) -> string()).
-spec(enable_cover/0 :: () -> 'ok' | {'error', any()}).
-spec(report_cover/0 :: () -> 'ok').
-spec(throw_on_error/2 ::
      (atom(), thunk({error, any()} | {ok, A} | A)) -> A). 
-spec(with_exit_handler/2 :: (thunk(A), thunk(A)) -> A).
-spec(filter_exit_map/2 :: (fun ((A) -> B), [A]) -> [B]).
-spec(with_user/2 :: (username(), thunk(A)) -> A).
-spec(with_vhost/2 :: (vhost(), thunk(A)) -> A).
-spec(with_user_and_vhost/3 :: (username(), vhost(), thunk(A)) -> A).
-spec(execute_mnesia_transaction/1 :: (thunk(A)) -> A).
-spec(ensure_ok/2 :: ('ok' | {'error', any()}, atom()) -> 'ok').
-spec(localnode/1 :: (atom()) -> erlang_node()).
-spec(tcp_name/3 :: (atom(), ip_address(), ip_port()) -> atom()).
-spec(intersperse/2 :: (A, [A]) -> [A]).
-spec(upmap/2 :: (fun ((A) -> B), [A]) -> [B]).
-spec(map_in_order/2 :: (fun ((A) -> B), [A]) -> [B]).
-spec(table_foreach/2 :: (fun ((any()) -> any()), atom()) -> 'ok').
-spec(dirty_read_all/1 :: (atom()) -> [any()]).
-spec(dirty_foreach_key/2 :: (fun ((any()) -> any()), atom()) ->
             'ok' | 'aborted').
-spec(dirty_dump_log/1 :: (string()) -> 'ok' | {'error', any()}).
-spec(append_file/2 :: (string(), string()) -> 'ok' | {'error', any()}).
-spec(ensure_parent_dirs_exist/1 :: (string()) -> 'ok').
-spec(format_stderr/2 :: (string(), [any()]) -> 'ok').
-spec(start_applications/1 :: ([atom()]) -> 'ok').
-spec(stop_applications/1 :: ([atom()]) -> 'ok').

-endif.

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
    protocol_error(frame_error, "cannot decode ~w",
                   [BinaryFields], MethodName).

protocol_error(Error, Explanation, Params) ->
    protocol_error(Error, Explanation, Params, none).

protocol_error(Error, Explanation, Params, Method) ->
    CompleteExplanation = lists:flatten(io_lib:format(Explanation, Params)),
    exit({amqp, Error, CompleteExplanation, Method}).

not_found(R) -> protocol_error(not_found, "no ~s", [rs(R)]).

get_config(Key) ->
    case dirty_read({rabbit_config, Key}) of
        {ok, {rabbit_config, Key, V}} -> {ok, V};
        Other -> Other
    end.

get_config(Key, DefaultValue) ->
    case get_config(Key) of
        {ok, V} -> V;
        {error, not_found} -> DefaultValue
    end.

set_config(Key, Value) ->
    ok = mnesia:dirty_write({rabbit_config, Key, Value}).

dirty_read(ReadSpec) ->
    case mnesia:dirty_read(ReadSpec) of
        [Result] -> {ok, Result};
        []       -> {error, not_found}
    end.

r(#resource{virtual_host = VHostPath}, Kind, Name)
  when is_binary(Name) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = Name};
r(VHostPath, Kind, Name) when is_binary(Name) andalso is_binary(VHostPath) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = Name}.

r(VHostPath, Kind) when is_binary(VHostPath) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = '_'}.

r_arg(#resource{virtual_host = VHostPath}, Kind, Table, Key) ->
    r_arg(VHostPath, Kind, Table, Key);
r_arg(VHostPath, Kind, Table, Key) ->
    case lists:keysearch(Key, 1, Table) of
        {value, {_, longstr, NameBin}} -> r(VHostPath, Kind, NameBin);
        false                          -> undefined
    end.

rs(#resource{virtual_host = VHostPath, kind = Kind, name = Name}) ->
    lists:flatten(io_lib:format("~s '~s' in vhost '~s'",
                                [Kind, Name, VHostPath])).

enable_cover() ->
    case cover:compile_beam_directory("ebin") of
        {error,Reason} -> {error,Reason};
        _ -> ok
    end.

report_cover() ->
    Dir = "cover/",
    ok = filelib:ensure_dir(Dir),
    lists:foreach(fun(F) -> file:delete(F) end,
                  filelib:wildcard(Dir ++ "*.html")),
    {ok, SummaryFile} = file:open(Dir ++ "summary.txt", [write]),
    {CT, NCT} =
        lists:foldl(
          fun(M,{CovTot, NotCovTot}) ->
                  {ok, {M, {Cov, NotCov}}} = cover:analyze(M, module),
                  ok = report_coverage_percentage(SummaryFile,
                                                  Cov, NotCov, M),
                  {ok,_} = cover:analyze_to_file(
                             M,
                             Dir ++ atom_to_list(M) ++ ".html",
                             [html]),
                  {CovTot+Cov, NotCovTot+NotCov}
          end,
          {0, 0},
          lists:sort(cover:modules())),
    ok = report_coverage_percentage(SummaryFile, CT, NCT, 'TOTAL'),
    ok = file:close(SummaryFile),
    ok.

report_coverage_percentage(File, Cov, NotCov, Mod) ->
    io:fwrite(File, "~6.2f ~p~n",
              [if
                   Cov+NotCov > 0 -> 100.0*Cov/(Cov+NotCov);
                   true -> 100.0
               end,
               Mod]).

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
        exit:{R, _} when R =:= noproc; R =:= normal; R =:= shutdown ->
            Handler()
    end.

filter_exit_map(F, L) ->
    Ref = make_ref(),
    lists:filter(fun (R) -> R =/= Ref end,
                 [with_exit_handler(
                    fun () -> Ref end,
                    fun () -> F(I) end) || I <- L]).

with_user(Username, Thunk) ->
    fun () ->
            case mnesia:read({rabbit_user, Username}) of
                [] ->
                    mnesia:abort({no_such_user, Username});
                [_U] ->
                    Thunk()
            end
    end.

with_vhost(VHostPath, Thunk) ->
    fun () ->
            case mnesia:read({rabbit_vhost, VHostPath}) of
                [] ->
                    mnesia:abort({no_such_vhost, VHostPath});
                [_V] ->
                    Thunk()
            end
    end.

with_user_and_vhost(Username, VHostPath, Thunk) ->
    with_user(Username, with_vhost(VHostPath, Thunk)).


execute_mnesia_transaction(TxFun) ->
    %% Making this a sync_transaction allows us to use dirty_read
    %% elsewhere and get a consistent result even when that read
    %% executes on a different node.
    case mnesia:sync_transaction(TxFun) of
        {atomic,  Result} -> Result;
        {aborted, Reason} -> throw({error, Reason})
    end.

ensure_ok(ok, _) -> ok;
ensure_ok({error, Reason}, ErrorTag) -> throw({error, {ErrorTag, Reason}}).

localnode(Name) ->
    %% This is horrible, but there doesn't seem to be a way to split a
    %% nodename into its constituent parts.
    list_to_atom(lists:append(atom_to_list(Name),
                              lists:dropwhile(fun (E) -> E =/= $@ end,
                                              atom_to_list(node())))).

tcp_name(Prefix, IPAddress, Port)
  when is_atom(Prefix) andalso is_number(Port) ->
    list_to_atom(
      lists:flatten(
        io_lib:format("~w_~s:~w",
                      [Prefix, inet_parse:ntoa(IPAddress), Port]))).

intersperse(_, []) -> [];
intersperse(_, [E]) -> [E];
intersperse(Sep, [E|T]) -> [E, Sep | intersperse(Sep, T)].

%% This is a modified version of Luke Gorrie's pmap -
%% http://lukego.livejournal.com/6753.html - that doesn't care about
%% the order in which results are received.
upmap(F, L) ->
    Parent = self(),
    Ref = make_ref(),
    [receive {Ref, Result} -> Result end
     || _ <- [spawn(fun() -> Parent ! {Ref, F(X)} end) || X <- L]].

map_in_order(F, L) ->
    lists:reverse(
      lists:foldl(fun (E, Acc) -> [F(E) | Acc] end, [], L)).

%% For each entry in a table, execute a function in a transaction.
%% This is often far more efficient than wrapping a tx around the lot.
%%
%% We ignore entries that have been modified or removed.
table_foreach(F, TableName) ->
    lists:foreach(
      fun (E) -> execute_mnesia_transaction(
                   fun () -> case mnesia:match_object(TableName, E, read) of
                                 [] -> ok;
                                 _  -> F(E)
                             end
                   end)
      end, dirty_read_all(TableName)),
    ok.

dirty_read_all(TableName) ->
    mnesia:dirty_select(TableName, [{'$1',[],['$1']}]).

dirty_foreach_key(F, TableName) ->
    dirty_foreach_key1(F, TableName, mnesia:dirty_first(TableName)).

dirty_foreach_key1(_F, _TableName, '$end_of_table') ->
    ok;
dirty_foreach_key1(F, TableName, K) ->
    case catch mnesia:dirty_next(TableName, K) of
        {'EXIT', _} ->
            aborted;
        NextKey ->
            F(K),
            dirty_foreach_key1(F, TableName, NextKey)
    end.

dirty_dump_log(FileName) ->
    {ok, LH} = disk_log:open([{name, dirty_dump_log}, {mode, read_only}, {file, FileName}]),
    dirty_dump_log1(LH, disk_log:chunk(LH, start)),
    disk_log:close(LH).

dirty_dump_log1(_LH, eof) ->
    io:format("Done.~n");
dirty_dump_log1(LH, {K, Terms}) ->
    io:format("Chunk: ~p~n", [Terms]),
    dirty_dump_log1(LH, disk_log:chunk(LH, K));
dirty_dump_log1(LH, {K, Terms, BadBytes}) ->
    io:format("Bad Chunk, ~p: ~p~n", [BadBytes, Terms]),
    dirty_dump_log1(LH, disk_log:chunk(LH, K)).


append_file(File, Suffix) ->
    case file:read_file_info(File) of
        {ok, FInfo}     -> append_file(File, FInfo#file_info.size, Suffix);
        {error, enoent} -> append_file(File, 0, Suffix);
        Error           -> Error
    end.

append_file(_, _, "") ->
    ok;
append_file(File, 0, Suffix) ->
    case file:open([File, Suffix], [append]) of
        {ok, Fd} -> file:close(Fd);
        Error    -> Error
    end;
append_file(File, _, Suffix) ->
    case file:read_file(File) of
        {ok, Data} -> file:write_file([File, Suffix], Data, [append]);
        Error      -> Error
    end.

ensure_parent_dirs_exist(Filename) ->
    case filelib:ensure_dir(Filename) of
        ok              -> ok;
        {error, Reason} -> 
            throw({error, {cannot_create_parent_dirs, Filename, Reason}})
    end.

format_stderr(Fmt, Args) ->
    case os:type() of
        {unix, _} ->
            Port = open_port({fd, 0, 2}, [out]),
            port_command(Port, io_lib:format(Fmt, Args)),
            port_close(Port);
        {win32, _} ->
            %% stderr on Windows is buffered and I can't figure out a
            %% way to trigger a fflush(stderr) in Erlang. So rather
            %% than risk losing output we write to stdout instead,
            %% which appears to be unbuffered.
            io:format(Fmt, Args)
    end,
    ok.

manage_applications(Iterate, Do, Undo, SkipError, ErrorTag, Apps) ->
    Iterate(fun (App, Acc) ->
                    case Do(App) of
                        ok -> [App | Acc];
                        {error, {SkipError, _}} -> Acc;
                        {error, Reason} ->
                            lists:foreach(Undo, Acc),
                            throw({error, {ErrorTag, App, Reason}})
                    end
            end, [], Apps),
    ok.

start_applications(Apps) ->
    manage_applications(fun lists:foldl/3,
                        fun application:start/1,
                        fun application:stop/1,
                        already_started,
                        cannot_start_application,
                        Apps).

stop_applications(Apps) ->
    manage_applications(fun lists:foldr/3,
                        fun application:stop/1,
                        fun application:start/1,
                        not_started,
                        cannot_stop_application,
                        Apps).

