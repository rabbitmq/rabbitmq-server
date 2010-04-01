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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
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
-export([die/1, frame_error/2, amqp_error/4,
         protocol_error/3, protocol_error/4]).
-export([not_found/1]).
-export([get_config/1, get_config/2, set_config/2]).
-export([dirty_read/1]).
-export([r/3, r/2, r_arg/4, rs/1]).
-export([enable_cover/0, report_cover/0]).
-export([enable_cover/1, report_cover/1]).
-export([throw_on_error/2, with_exit_handler/2, filter_exit_map/2]).
-export([with_user/2, with_vhost/2, with_user_and_vhost/3]).
-export([execute_mnesia_transaction/1]).
-export([ensure_ok/2]).
-export([makenode/1, nodeparts/1, cookie_hash/0, tcp_name/3]).
-export([intersperse/2, upmap/2, map_in_order/2]).
-export([table_fold/3]).
-export([dirty_read_all/1, dirty_foreach_key/2, dirty_dump_log/1]).
-export([read_term_file/1, write_term_file/2]).
-export([append_file/2, ensure_parent_dirs_exist/1]).
-export([format_stderr/2]).
-export([start_applications/1, stop_applications/1]).
-export([unfold/2, ceil/1, queue_fold/3]).
-export([sort_field_table/1]).
-export([pid_to_string/1, string_to_pid/1]).
-export([version_compare/2, version_compare/3]).

-import(mnesia).
-import(lists).
-import(cover).
-import(disk_log).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-include_lib("kernel/include/inet.hrl").

-type(ok_or_error() :: 'ok' | {'error', any()}).

-spec(method_record_type/1 :: (tuple()) -> atom()).
-spec(polite_pause/0 :: () -> 'done').
-spec(polite_pause/1 :: (non_neg_integer()) -> 'done').
-spec(die/1 :: (atom()) -> no_return()).
-spec(frame_error/2 :: (atom(), binary()) -> no_return()).
-spec(amqp_error/4 :: (atom(), string(), [any()], atom()) -> amqp_error()).
-spec(protocol_error/3 :: (atom(), string(), [any()]) -> no_return()).
-spec(protocol_error/4 :: (atom(), string(), [any()], atom()) -> no_return()).
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
-spec(enable_cover/0 :: () -> ok_or_error()).
-spec(report_cover/0 :: () -> 'ok').
-spec(enable_cover/1 :: (string()) -> ok_or_error()).
-spec(report_cover/1 :: (string()) -> 'ok').
-spec(throw_on_error/2 ::
      (atom(), thunk({error, any()} | {ok, A} | A)) -> A).
-spec(with_exit_handler/2 :: (thunk(A), thunk(A)) -> A).
-spec(filter_exit_map/2 :: (fun ((A) -> B), [A]) -> [B]).
-spec(with_user/2 :: (username(), thunk(A)) -> A).
-spec(with_vhost/2 :: (vhost(), thunk(A)) -> A).
-spec(with_user_and_vhost/3 :: (username(), vhost(), thunk(A)) -> A).
-spec(execute_mnesia_transaction/1 :: (thunk(A)) -> A).
-spec(ensure_ok/2 :: (ok_or_error(), atom()) -> 'ok').
-spec(makenode/1 :: ({string(), string()} | string()) -> erlang_node()).
-spec(nodeparts/1 :: (erlang_node() | string()) -> {string(), string()}).
-spec(cookie_hash/0 :: () -> string()).
-spec(tcp_name/3 :: (atom(), ip_address(), ip_port()) -> atom()).
-spec(intersperse/2 :: (A, [A]) -> [A]).
-spec(upmap/2 :: (fun ((A) -> B), [A]) -> [B]).
-spec(map_in_order/2 :: (fun ((A) -> B), [A]) -> [B]).
-spec(table_fold/3 :: (fun ((any(), A) -> A), A, atom()) -> A).
-spec(dirty_read_all/1 :: (atom()) -> [any()]).
-spec(dirty_foreach_key/2 :: (fun ((any()) -> any()), atom()) ->
             'ok' | 'aborted').
-spec(dirty_dump_log/1 :: (string()) -> ok_or_error()).
-spec(read_term_file/1 :: (string()) -> {'ok', [any()]} | {'error', any()}).
-spec(write_term_file/2 :: (string(), [any()]) -> ok_or_error()).
-spec(append_file/2 :: (string(), string()) -> ok_or_error()).
-spec(ensure_parent_dirs_exist/1 :: (string()) -> 'ok').
-spec(format_stderr/2 :: (string(), [any()]) -> 'ok').
-spec(start_applications/1 :: ([atom()]) -> 'ok').
-spec(stop_applications/1 :: ([atom()]) -> 'ok').
-spec(unfold/2  :: (fun ((A) -> ({'true', B, A} | 'false')), A) -> {[B], A}).
-spec(ceil/1 :: (number()) -> number()).
-spec(queue_fold/3 :: (fun ((any(), B) -> B), B, queue()) -> B).
-spec(sort_field_table/1 :: (amqp_table()) -> amqp_table()).
-spec(pid_to_string/1 :: (pid()) -> string()).
-spec(string_to_pid/1 :: (string()) -> pid()).

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
    protocol_error(frame_error, "cannot decode ~w", [BinaryFields], MethodName).

amqp_error(Name, ExplanationFormat, Params, Method) ->
    Explanation = lists:flatten(io_lib:format(ExplanationFormat, Params)),
    #amqp_error{name = Name, explanation = Explanation, method = Method}.

protocol_error(Name, ExplanationFormat, Params) ->
    protocol_error(Name, ExplanationFormat, Params, none).

protocol_error(Name, ExplanationFormat, Params, Method) ->
    exit(amqp_error(Name, ExplanationFormat, Params, Method)).

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
    enable_cover(".").

enable_cover([Root]) when is_atom(Root) ->
    enable_cover(atom_to_list(Root));
enable_cover(Root) ->
    case cover:compile_beam_directory(filename:join(Root, "ebin")) of
        {error,Reason} -> {error,Reason};
        _ -> ok
    end.

report_cover() ->
    report_cover(".").

report_cover([Root]) when is_atom(Root) ->
    report_cover(atom_to_list(Root));
report_cover(Root) ->
    Dir = filename:join(Root, "cover"),
    ok = filelib:ensure_dir(filename:join(Dir,"junk")),
    lists:foreach(fun(F) -> file:delete(F) end,
                  filelib:wildcard(filename:join(Dir, "*.html"))),
    {ok, SummaryFile} = file:open(filename:join(Dir, "summary.txt"), [write]),
    {CT, NCT} =
        lists:foldl(
          fun(M,{CovTot, NotCovTot}) ->
                  {ok, {M, {Cov, NotCov}}} = cover:analyze(M, module),
                  ok = report_coverage_percentage(SummaryFile,
                                                  Cov, NotCov, M),
                  {ok,_} = cover:analyze_to_file(
                             M,
                             filename:join(Dir, atom_to_list(M) ++ ".html"),
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
    case worker_pool:submit({mnesia, sync_transaction, [TxFun]}) of
        {atomic,  Result} -> Result;
        {aborted, Reason} -> throw({error, Reason})
    end.

ensure_ok(ok, _) -> ok;
ensure_ok({error, Reason}, ErrorTag) -> throw({error, {ErrorTag, Reason}}).

makenode({Prefix, Suffix}) ->
    list_to_atom(lists:append([Prefix, "@", Suffix]));
makenode(NodeStr) ->
    makenode(nodeparts(NodeStr)).

nodeparts(Node) when is_atom(Node) ->
    nodeparts(atom_to_list(Node));
nodeparts(NodeStr) ->
    case lists:splitwith(fun (E) -> E =/= $@ end, NodeStr) of
        {Prefix, []}     -> {_, Suffix} = nodeparts(node()),
                            {Prefix, Suffix};
        {Prefix, Suffix} -> {Prefix, tl(Suffix)}
    end.

cookie_hash() ->
    base64:encode_to_string(erlang:md5(atom_to_list(erlang:get_cookie()))).

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
%%
%% WARNING: This is is deliberately lightweight rather than robust -- if F
%% throws, upmap will hang forever, so make sure F doesn't throw!
upmap(F, L) ->
    Parent = self(),
    Ref = make_ref(),
    [receive {Ref, Result} -> Result end
     || _ <- [spawn(fun() -> Parent ! {Ref, F(X)} end) || X <- L]].

map_in_order(F, L) ->
    lists:reverse(
      lists:foldl(fun (E, Acc) -> [F(E) | Acc] end, [], L)).

%% Fold over each entry in a table, executing the cons function in a
%% transaction.  This is often far more efficient than wrapping a tx
%% around the lot.
%%
%% We ignore entries that have been modified or removed.
table_fold(F, Acc0, TableName) ->
    lists:foldl(
      fun (E, Acc) -> execute_mnesia_transaction(
                   fun () -> case mnesia:match_object(TableName, E, read) of
                                 [] -> Acc;
                                 _  -> F(E, Acc)
                             end
                   end)
      end, Acc0, dirty_read_all(TableName)).

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
    {ok, LH} = disk_log:open([{name, dirty_dump_log},
                              {mode, read_only},
                              {file, FileName}]),
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


read_term_file(File) -> file:consult(File).

write_term_file(File, Terms) ->
    file:write_file(File, list_to_binary([io_lib:format("~w.~n", [Term]) ||
                                             Term <- Terms])).

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

queue_fold(Fun, Init, Q) ->
    case queue:out(Q) of
        {empty, _Q}      -> Init;
        {{value, V}, Q1} -> queue_fold(Fun, Fun(V, Init), Q1)
    end.

%% Sorts a list of AMQP table fields as per the AMQP spec
sort_field_table(Arguments) ->
    lists:keysort(1, Arguments).

%% This provides a string representation of a pid that is the same
%% regardless of what node we are running on. The representation also
%% permits easy identification of the pid's node.
pid_to_string(Pid) when is_pid(Pid) ->
    %% see http://erlang.org/doc/apps/erts/erl_ext_dist.html (8.10 and
    %% 8.7)
    <<131,103,100,NodeLen:16,NodeBin:NodeLen/binary,Id:32,Ser:32,_Cre:8>>
        = term_to_binary(Pid),
    Node = binary_to_term(<<131,100,NodeLen:16,NodeBin:NodeLen/binary>>),
    lists:flatten(io_lib:format("<~w.~B.~B>", [Node, Id, Ser])).

%% inverse of above
string_to_pid(Str) ->
    ErrorFun = fun () -> throw({error, {invalid_pid_syntax, Str}}) end,
    %% TODO: simplify this code by using the 're' module, once we drop
    %% support for R11
    %%
    %% 1) sanity check
    %% The \ before the trailing $ is only there to keep emacs
    %% font-lock from getting confused.
    case regexp:first_match(Str, "^<.*\\.[0-9]+\\.[0-9]+>\$") of
        {match, _, _} ->
            %% 2) strip <>
            Str1 = string:substr(Str, 2, string:len(Str) - 2),
            %% 3) extract three constituent parts, taking care to
            %% handle dots in the node part (hence the reverse and concat)
            [SerStr, IdStr | Rest] = lists:reverse(string:tokens(Str1, ".")),
            NodeStr = lists:concat(lists:reverse(Rest)),
            %% 4) construct a triple term from the three parts
            TripleStr = lists:flatten(io_lib:format("{~s,~s,~s}.",
                                                    [NodeStr, IdStr, SerStr])),
            %% 5) parse the triple
            Tokens = case erl_scan:string(TripleStr) of
                         {ok, Tokens1, _} -> Tokens1;
                         {error, _, _}    -> ErrorFun()
                     end,
            Term = case erl_parse:parse_term(Tokens) of
                       {ok, Term1} -> Term1;
                       {error, _}  -> ErrorFun()
                   end,
            {Node, Id, Ser} =
                case Term of
                    {Node1, Id1, Ser1} when is_atom(Node1) andalso
                                            is_integer(Id1) andalso
                                            is_integer(Ser1) ->
                        Term;
                    _ ->
                        ErrorFun()
                end,
            %% 6) turn the triple into a pid - see pid_to_string
            <<131,NodeEnc/binary>> = term_to_binary(Node),
            binary_to_term(<<131,103,NodeEnc/binary,Id:32,Ser:32,0:8>>);
        nomatch ->
            ErrorFun();
        Error ->
            %% invalid regexp - shouldn't happen
            throw(Error)
    end.

version_compare(A, B, lte) ->
    case version_compare(A, B) of
        eq -> true;
        lt -> true;
        gt -> false
    end;
version_compare(A, B, gte) ->
    case version_compare(A, B) of
        eq -> true;
        gt -> true;
        lt -> false
    end;
version_compare(A, B, Result) ->
    Result =:= version_compare(A, B).

version_compare([], []) ->
    eq;
version_compare([], _ ) ->
    lt; %% 2.3 < 2.3.1
version_compare(_ , []) ->
    gt; %% 2.3.1 > 2.3
version_compare(A,  B) ->
    {AStr, ATl} = lists:splitwith(fun (X) -> X =/= $. end, A),
    {BStr, BTl} = lists:splitwith(fun (X) -> X =/= $. end, B),
    ANum = list_to_integer(AStr),
    BNum = list_to_integer(BStr),
    if ANum =:= BNum -> ATl1 = lists:dropwhile(fun (X) -> X =:= $. end, ATl),
                        BTl1 = lists:dropwhile(fun (X) -> X =:= $. end, BTl),
                        version_compare(ATl1, BTl1);
       ANum < BNum   -> lt;
       ANum > BNum   -> gt
    end.
