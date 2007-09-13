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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_misc).
-include("rabbit.hrl").

-export([method_record_type/1, polite_pause/0, polite_pause/1, die/1, die/2]).
-export([frame_error/2]).
-export([strict_ticket_checking/0]).
-export([get_config/1, get_config/2, set_config/2]).
-export([clean_read/1, clean_read_many/1]).
-export([r/3, r/2]).
-export([enable_cover/0, report_cover/0]).
-export([with_user/2, with_vhost/2, with_realm/2, with_user_and_vhost/3]).
-export([execute_mnesia_transaction/2, execute_simple_mnesia_transaction/1]).
-export([with_error_handler/2, ensure_ok/2]).
-export([localnode/1, tcp_name/3]).
-export([intersperse/2]).
-export([dirty_read_all/1, dirty_dump_log/1]).

-import(mnesia).
-import(lists).
-import(cover).
-import(disk_log).

method_record_type(Record) ->
    element(1, Record).

polite_pause() ->
    polite_pause(3000).

polite_pause(N) ->
    receive
    after N ->
            done
    end.

die(Reason) ->
    exit({amqp, Reason, none}).

die(Reason, MethodNameOrClassAndId) ->
    exit({amqp, Reason, MethodNameOrClassAndId}).

boolean_config_param(Name, TrueValue, FalseValue, DefaultValue) ->
    ActualValue = get_config(Name, DefaultValue),
    if
        ActualValue == TrueValue ->
            true;
        ActualValue == FalseValue ->
            false;
        true ->
            rabbit_log:error("Bad setting for config param '~p' ~p; legal values are '~p', '~p'~n",
                             [Name, ActualValue, TrueValue, FalseValue]),
            DefaultValue == TrueValue
    end.

strict_ticket_checking() ->
    boolean_config_param(strict_ticket_checking, enabled, disabled, disabled).

frame_error(MethodName, BinaryFields) ->
    rabbit_log:error("Frame error: ~p, ~p~n", [MethodName, BinaryFields]),
    die(frame_error, MethodName).

get_config(Key) ->
    case clean_read({rabbit_config, Key}) of
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

clean_read(ReadSpec) ->
    case mnesia:dirty_read(ReadSpec) of
        [] ->
            case mnesia:transaction(fun () -> mnesia:read(ReadSpec) end) of
                {atomic, []} ->
                    {error, not_found};
                {atomic, [Result]} ->
                    {ok, Result}
            end;
        [Result] ->
            {ok, Result}
    end.

clean_read_many(ReadSpec) ->
    {atomic, Result} = mnesia:transaction(fun () -> mnesia:read(ReadSpec) end),
    Result.

r(#resource{virtual_host = VHostPath}, Kind, Name)
  when is_binary(Name) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = Name};
r(VHostPath, Kind, Name) when is_binary(Name) andalso is_binary(VHostPath) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = Name}.

r(VHostPath, Kind) when is_binary(VHostPath) ->
    #resource{virtual_host = VHostPath, kind = Kind, name = '_'}.

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

with_user(Username, Thunk) ->
    fun () ->
            case mnesia:read({user, Username}) of
                [] ->
                    mnesia:abort({no_such_user, Username});
                [_U] ->
                    Thunk()
            end
    end.

with_vhost(VHostPath, Thunk) ->
    fun () ->
            case mnesia:read({vhost, VHostPath}) of
                [] -> 
                    mnesia:abort({no_such_vhost, VHostPath});
                [_V] ->
                    Thunk()
            end
    end.

with_realm(Name = #resource{virtual_host = VHostPath, kind = realm},
           Thunk) ->
    fun () ->
            case mnesia:read({realm, Name}) of
                [] ->
                    mnesia:abort({no_such_realm, Name});
                [_R] ->
                    case mnesia:match_object(
                           #vhost_realm{virtual_host = VHostPath,
                                        realm = Name}) of
                        [] ->
                            %% This should never happen
                            mnesia:abort({no_such_realm, Name});
                        [_VR] ->
                            Thunk()
                    end
            end
    end.

with_user_and_vhost(Username, VHostPath, Thunk) ->
    with_user(Username, with_vhost(VHostPath, Thunk)).

execute_mnesia_transaction(TxFun, SuccessFun) ->
    case mnesia:transaction(TxFun) of
        {atomic, Result} -> SuccessFun(Result);
        {aborted, Reason} -> throw({error, Reason})
    end.

execute_simple_mnesia_transaction(TxFun) ->
    execute_mnesia_transaction(TxFun, fun (Result) -> Result end).


%% Handler is called for any errors in Thunk. The error, including the
%% original stack trace, are propagated.
with_error_handler(HandlerThunk, Thunk) ->
    try
        Res = Thunk(),
        put(success_signal, success),
        Res
    after
        case get(success_signal) of
            success -> erase(success_signal);
            undefined -> HandlerThunk()
        end
    end.

ensure_ok(ok, _) -> ok;
ensure_ok({error, Reason}, ErrorTag) -> throw({error, {ErrorTag, Reason}}).

localnode(Name) ->
    %% This is horrible, but there doesn't seem to be a way to split a
    %% nodename into its constituent parts.
    list_to_atom(lists:append(atom_to_list(Name),
                              lists:dropwhile(fun (E) -> E =/= $@ end,
                                              atom_to_list(node())))).

tcp_name(Prefix, IPAddress, Port) ->
    list_to_atom(
      lists:flatten(
        io_lib:format("~p_~s:~p",
                      [Prefix, inet_parse:ntoa(IPAddress), Port]))).

intersperse(_, []) -> [];
intersperse(_, [E]) -> [E];
intersperse(Sep, [E|T]) -> [E, Sep] ++ intersperse(Sep, T).

dirty_read_all(TableName) ->
    mnesia:dirty_select(TableName, [{'$1',[],['$1']}]).

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
