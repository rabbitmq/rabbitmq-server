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

%% So, assuming you're on some debian linux type system,
%% apt-get install postgresql odbc-postgresql unixodbc unixodbc-bin
%% sudo odbcinst -i -d -f /usr/share/psqlodbc/odbcinst.ini.template

%% Now set up in postgresql a user and a database that user can
%% access. For example, the database could be called rabbit_db_queue
%% and the username could be rabbit and the password could be rabbit.

%% sudo ODBCConfig
%% set up a system wide dsn with the above settings in it.
%% now drop into the erlang shell, and you should not get an error after:

%% > odbc:start().
%% < ok.
%% > odbc:connect("DSN=rabbit_db_queue", []).
%% < {ok,<0.325.0>}
%% ( replace rabbit_db_queue with the name of your DSN that you configured )

%% the connection string (eg "DSN=rabbit_db_queue") is what you pass
%% to start_link. Don't just pass the DSN name.

-module(rabbit_db_queue).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([publish/3, deliver/1, phantom_deliver/1, ack/2, tx_publish/2,
         tx_commit/3, tx_cancel/1, requeue/2, purge/1]).

-export([stop/0, stop_and_obliterate/0]).

-include("rabbit.hrl").

-define(SERVER, ?MODULE).

%% ---- SPECS ----

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).

-spec(start_link/1 :: (non_neg_integer()) ->
              {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(publish/3 :: (queue_name(), msg_id(), binary()) -> 'ok').
-spec(deliver/1 :: (queue_name()) ->
             {'empty' | {msg_id(), binary(), non_neg_integer(),
                         bool(), {msg_id(), seq_id()}}}).
-spec(phantom_deliver/1 :: (queue_name()) ->
             { 'empty' | {msg_id(), bool(), {msg_id(), seq_id()}}}).
-spec(ack/2 :: (queue_name(), [{msg_id(), seq_id()}]) -> 'ok').
-spec(tx_publish/2 :: (msg_id(), binary()) -> 'ok').
-spec(tx_commit/3 :: (queue_name(), [msg_id()], [seq_id()]) -> 'ok').
-spec(tx_cancel/1 :: ([msg_id()]) -> 'ok').
-spec(requeue/2 :: (queue_name(), [seq_id()]) -> 'ok').
-spec(purge/1 :: (queue_name()) -> non_neg_integer()).
-spec(stop/0 :: () -> 'ok').
-spec(stop_and_obliterate/0 :: () -> 'ok').

-endif.

%% ---- PUBLIC API ----

start_link(DSN) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [DSN], []).

publish(Q, MsgId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {publish, Q, MsgId, Msg}).

deliver(Q) ->
    gen_server:call(?SERVER, {deliver, Q}, infinity).

phantom_deliver(Q) ->
    gen_server:call(?SERVER, {phantom_deliver, Q}).

ack(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server:cast(?SERVER, {ack, Q, MsgSeqIds}).

tx_publish(MsgId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {tx_publish, MsgId, Msg}).

tx_commit(Q, PubMsgIds, AckSeqIds) when is_list(PubMsgIds) andalso is_list(AckSeqIds) ->
    gen_server:call(?SERVER, {tx_commit, Q, PubMsgIds, AckSeqIds}, infinity).

tx_cancel(MsgIds) when is_list(MsgIds) ->
    gen_server:cast(?SERVER, {tx_cancel, MsgIds}).

requeue(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server:cast(?SERVER, {requeue, Q, MsgSeqIds}).

purge(Q) ->
    gen_server:call(?SERVER, {purge, Q}).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

stop_and_obliterate() ->
    gen_server:call(?SERVER, stop_vaporise, infinity).

%% ---- GEN-SERVER INTERNAL API ----
-record(dbstate, { db_conn }).

init([DSN]) ->
    process_flag(trap_exit, true),
    odbc:start(),
    {ok, Conn} = odbc:connect(DSN, [{auto_commit, off}, {tuple_row, on},
                                    {scrollable_cursors, off}, {trace_driver, off}]),
    State = #dbstate { db_conn = Conn },
    compact_already_delivered(State),
    {ok, State}.

handle_call({deliver, Q}, _From, State) ->
    {ok, Result, State1} = internal_deliver(Q, true, State),
    {reply, Result, State1};
handle_call({phantom_deliver, Q}, _From, State) ->
    {ok, Result, State1} = internal_deliver(Q, false, State),
    {reply, Result, State1};
handle_call({tx_commit, Q, PubMsgIds, AckSeqIds}, _From, State) ->
    {ok, State1} = internal_tx_commit(Q, PubMsgIds, AckSeqIds, State),
    {reply, ok, State1};
handle_call({purge, Q}, _From, State) ->
    {ok, Count, State1} = internal_purge(Q, State),
    {reply, Count, State1};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}; %% gen_server now calls terminate
handle_call(stop_vaporise, _From, State = #dbstate { db_conn = Conn }) ->
    odbc:sql_query(Conn, "delete from ledger"),
    odbc:sql_query(Conn, "delete from sequence"),
    odbc:sql_query(Conn, "delete from message"),
    odbc:commit(Conn, commit),
    {stop, normal, ok, State}.
    %% gen_server now calls terminate, which then calls shutdown

handle_cast({publish, Q, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_publish(Q, MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({ack, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_ack(Q, MsgSeqIds, State),
    {noreply, State1};
handle_cast({tx_publish, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({tx_cancel, MsgIds}, State) ->
    {ok, State1} = internal_tx_cancel(MsgIds, State),
    {noreply, State1};
handle_cast({requeue, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_requeue(Q, MsgSeqIds, State),
    {noreply, State1}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    shutdown(State).

shutdown(State = #dbstate { db_conn = Conn }) ->
    odbc:disconnect(Conn),
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---- UTILITY FUNCTIONS ----

binary_to_escaped_string(Bin) when is_binary(Bin) ->
    "E'" ++ lists:flatten(lists:reverse(binary_to_escaped_string(Bin, []))) ++ "'".

binary_to_escaped_string(<<>>, Acc) ->
    Acc;
binary_to_escaped_string(<<Byte:8, Rest/binary>>, Acc) ->
    binary_to_escaped_string(Rest, [escape_byte(Byte) | Acc]).

escape_byte(39) ->
    "\\\\047";
escape_byte(92) ->
    "\\\\134";
escape_byte(B) when B > 31 andalso B < 127 ->
    B;
escape_byte(B) ->
    case io_lib:format("~.8B", [B]) of
        O1 = [[_]] ->
            "\\\\00" ++ O1;
        O2 = [[_,_]] ->
            "\\\\0" ++ O2;
        O3 = [[_,_,_]] ->
            "\\\\" ++ O3
    end.

escaped_string_to_binary(Str) when is_list(Str) ->
    list_to_binary(lists:reverse(escaped_string_to_binary(Str, []))).

escaped_string_to_binary([], Acc) ->
    Acc;
escaped_string_to_binary([$\\,$\\|Rest], Acc) ->
    escaped_string_to_binary(Rest, [$\\ | Acc]);
escaped_string_to_binary([$\\,A,B,C|Rest], Acc) ->
    escaped_string_to_binary(Rest, [(list_to_integer([A])*64) +
                                    (list_to_integer([B])*8) +
                                    list_to_integer([C])
                                   | Acc]);
escaped_string_to_binary([C|Rest], Acc) ->
    escaped_string_to_binary(Rest, [C|Acc]).

hex_string_to_binary(Str) when is_list(Str) ->
    list_to_binary(lists:reverse(hex_string_to_binary(Str, []))).

hex_string_to_binary([], Acc) ->
    Acc;
hex_string_to_binary([A,B|Rest], Acc) ->
    {ok, [N], []} = io_lib:fread("~16u", [A,B]),
    hex_string_to_binary(Rest, [N | Acc]).

%% ---- INTERNAL RAW FUNCTIONS ----

internal_deliver(Q, ReadMsg, State = #dbstate { db_conn = Conn }) ->
    QStr = binary_to_escaped_string(term_to_binary(Q)),
    case odbc:sql_query(Conn, "select next_read from sequence where queue = " ++ QStr) of
        {selected, _, []} ->
            odbc:commit(Conn, commit),
            {ok, empty, State};
        {selected, _, [{ReadSeqId}]} ->
            case odbc:sql_query(Conn, "select is_delivered, msg_id from ledger where queue = " ++ QStr ++
                                " and seq_id = " ++ integer_to_list(ReadSeqId)) of
                {selected, _, []} ->
                    {ok, empty, State};
                {selected, _, [{IsDeliveredStr, MsgIdStr}]} ->
                    IsDelivered = IsDeliveredStr /= "0",
                    if IsDelivered -> ok;
                       true -> odbc:sql_query(Conn, "update ledger set is_delivered = true where queue = " ++
                                              QStr ++ " and seq_id = " ++ integer_to_list(ReadSeqId))
                    end,
                    MsgId = binary_to_term(hex_string_to_binary(MsgIdStr)),
                    %% yeah, this is really necessary. sigh
                    MsgIdStr2 = binary_to_escaped_string(term_to_binary(MsgId)),
                    odbc:sql_query(Conn, "update sequence set next_read = " ++ integer_to_list(ReadSeqId + 1) ++
                                   " where queue = " ++ QStr),
                    if ReadMsg ->
                            {selected, _, [{MsgBodyStr}]} =
                                odbc:sql_query(Conn, "select msg from message where msg_id = " ++ MsgIdStr2),
                            odbc:commit(Conn, commit),
                            MsgBody = hex_string_to_binary(MsgBodyStr),
                            BodySize = size(MsgBody),
                            {ok, {MsgId, MsgBody, BodySize, IsDelivered, {MsgId, ReadSeqId}}, State};
                       true ->
                            odbc:commit(Conn, commit),
                            {ok, {MsgId, IsDelivered, {MsgId, ReadSeqId}}, State}
                    end
            end
    end.

internal_ack(Q, MsgSeqIds, State) ->
    remove_messages(Q, MsgSeqIds, true, State).

%% Q is only needed if LedgerDelete /= false
%% called from tx_cancel with LedgerDelete = false
%% called from internal_tx_cancel with LedgerDelete = true
%% called from ack with LedgerDelete = true
remove_messages(Q, MsgSeqIds, LedgerDelete, State = #dbstate { db_conn = Conn }) ->
    QStr = binary_to_escaped_string(term_to_binary(Q)),
    lists:foreach(
      fun ({MsgId, SeqId}) ->
              MsgIdStr = binary_to_escaped_string(term_to_binary(MsgId)),
              {selected, _, [{RefCount}]} =
                  odbc:sql_query(Conn, "select ref_count from message where msg_id = " ++
                                 MsgIdStr),
              case RefCount of
                  1 -> odbc:sql_query(Conn, "delete from message where msg_id = " ++
                                      MsgIdStr);
                  _ -> odbc:sql_query(Conn, "update message set ref_count = " ++
                                      integer_to_list(RefCount - 1) ++ " where msg_id = " ++
                                      MsgIdStr)
              end,
              if LedgerDelete ->
                      odbc:sql_query(Conn, "delete from ledger where queue = " ++
                                     QStr ++ " and seq_id = " ++ integer_to_list(SeqId));
                 true -> ok
              end
      end, MsgSeqIds),
    odbc:commit(Conn, commit),
    {ok, State}.

internal_tx_publish(MsgId, MsgBody, State = #dbstate { db_conn = Conn }) ->
    MsgIdStr = binary_to_escaped_string(term_to_binary(MsgId)),
    MsgStr = binary_to_escaped_string(MsgBody),
    case odbc:sql_query(Conn, "select ref_count from message where msg_id = " ++ MsgIdStr) of
        {selected, _, []} ->
            odbc:sql_query(Conn, "insert into message (msg_id, msg, ref_count) values (" ++
                           MsgIdStr ++ ", " ++ MsgStr ++ ", 1)");
        {selected, _, [{RefCount}]} ->
            odbc:sql_query(Conn, "update message set ref_count = " ++
                           integer_to_list(RefCount + 1) ++ " where msg_id = " ++ MsgIdStr)
    end,
    odbc:commit(Conn, commit),
    {ok, State}.

internal_tx_commit(Q, PubMsgIds, AckSeqIds, State = #dbstate { db_conn = Conn }) ->
    QStr = binary_to_escaped_string(term_to_binary(Q)),
    {InsertOrUpdate, NextWrite} =
        case odbc:sql_query(Conn, "select next_write from sequence where queue = " ++ QStr) of
            {selected, _, []} -> {insert, 0};
            {selected, _, [{NextWrite2}]} -> {update, NextWrite2}
        end,
    NextWrite3 =
        lists:foldl(fun (MsgId, WriteSeqInteger) ->
                            MsgIdStr = binary_to_escaped_string(term_to_binary(MsgId)),
                            odbc:sql_query(Conn,
                                           "insert into ledger (queue, seq_id, is_delivered, msg_id) values (" ++
                                           QStr ++ ", " ++ integer_to_list(WriteSeqInteger) ++ ", false, " ++
                                           MsgIdStr ++ ")"),
                            WriteSeqInteger + 1
                    end, NextWrite, PubMsgIds),
    case InsertOrUpdate of
        update -> odbc:sql_query(Conn, "update sequence set next_write = " ++ integer_to_list(NextWrite3) ++
                                 " where queue = " ++ QStr);
        insert -> odbc:sql_query(Conn, "insert into sequence (queue, next_read, next_write) values (" ++
                                 QStr ++ ", 0, " ++ integer_to_list(NextWrite3) ++ ")")
    end,
    odbc:commit(Conn, commit),
    remove_messages(Q, AckSeqIds, true, State),
    {ok, State}.

internal_publish(Q, MsgId, MsgBody, State = #dbstate { db_conn = Conn }) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    MsgIdStr = binary_to_escaped_string(term_to_binary(MsgId)),
    QStr = binary_to_escaped_string(term_to_binary(Q)),
    NextWrite =
        case odbc:sql_query(Conn, "select next_write from sequence where queue = " ++ QStr) of
            {selected, _, []} ->
                odbc:sql_query(Conn,
                               "insert into sequence (queue, next_read, next_write) values (" ++
                               QStr ++ ", 0, 1)"),
                0;
            {selected, _, [{NextWrite2}]} ->
                odbc:sql_query(Conn, "update sequence set next_write = " ++ integer_to_list(1 + NextWrite2) ++
                               " where queue = " ++ QStr),
                NextWrite2
        end,
    odbc:sql_query(Conn, "insert into ledger (queue, seq_id, is_delivered, msg_id) values (" ++
                   QStr ++ ", " ++ integer_to_list(NextWrite) ++ ", false, " ++ MsgIdStr ++ ")"),
    odbc:commit(Conn, commit),
    {ok, State1}.

internal_tx_cancel(MsgIds, State) ->
    MsgSeqIds = lists:zip(MsgIds, lists:duplicate(length(MsgIds), undefined)),
    remove_messages(undefined, MsgSeqIds, false, State).

internal_requeue(Q, MsgSeqIds, State = #dbstate { db_conn = Conn }) ->
    QStr = binary_to_escaped_string(term_to_binary(Q)),
    {selected, _, [{WriteSeqId}]} =
        odbc:sql_query(Conn, "select next_write from sequence where queue = " ++ QStr),
    WriteSeqId2 =
        lists:foldl(
          fun ({_MsgId, SeqId}, NextWriteSeqId) ->
                  odbc:sql_query(Conn, "update ledger set seq_id = " ++ integer_to_list(NextWriteSeqId) ++
                                 " where seq_id = " ++ integer_to_list(SeqId) ++ " and queue = " ++ QStr),
                  NextWriteSeqId + 1
          end, WriteSeqId, MsgSeqIds),
    odbc:sql_query(Conn, "update sequence set next_write = " ++ integer_to_list(WriteSeqId2) ++
                   " where queue = " ++ QStr),
    odbc:commit(Conn, commit),
    {ok, State}.
                                 

compact_already_delivered(#dbstate { db_conn = Conn }) ->
    {selected, _, Seqs} = odbc:sql_query(Conn, "select queue, next_read from sequence"),
    lists:foreach(
      fun ({QHexStr, ReadSeqId}) ->
              Q = binary_to_term(hex_string_to_binary(QHexStr)),
              QStr = binary_to_escaped_string(term_to_binary(Q)),
              case odbc:sql_query(Conn, "select min(seq_id) from ledger where queue = "
                                  ++ QStr) of
                  {selected, _, []} -> ok;
                  {selected, _, [{null}]} -> ok; %% AGH!
                  {selected, _, [{Min}]} ->
                      Gap = shuffle_up(Conn, QStr, Min - 1, ReadSeqId - 1, 0),
                      odbc:sql_query(Conn, "update sequence set next_read = " ++
                                     integer_to_list(Min + Gap) ++
                                     " where queue = " ++ QStr)
              end
      end, Seqs),
    odbc:commit(Conn, commit).

shuffle_up(_Conn, _QStr, SeqId, SeqId, Gap) ->
    Gap;
shuffle_up(Conn, QStr, BaseSeqId, SeqId, Gap) ->
    GapInc =
        case odbc:sql_query(Conn, "select count(1) from ledger where queue = " ++
                            QStr ++ " and seq_id = " ++ integer_to_list(SeqId)) of
            {selected, _, [{"0"}]} ->
                1;
            {selected, _, [{"1"}]} ->
                if Gap =:= 0 -> ok;
                   true -> odbc:sql_query(Conn, "update ledger set seq_id = " ++
                                          integer_to_list(SeqId + Gap) ++ " where seq_id = " ++
                                          integer_to_list(SeqId) ++ " and queue = " ++ QStr)
                end,
                0
        end,
    shuffle_up(Conn, QStr, BaseSeqId, SeqId - 1, Gap + GapInc).

internal_purge(Q, State = #dbstate { db_conn = Conn }) ->
    QStr = binary_to_escaped_string(term_to_binary(Q)),
    case odbc:sql_query(Conn, "select next_read from sequence where queue = " ++ QStr) of
        {selected, _, []} ->
            odbc:commit(Conn, commit),
            {ok, 0, State};
        {selected, _, [{ReadSeqId}]} ->
            odbc:sql_query(Conn, "update sequence set next_read = next_write where queue = " ++ QStr),
            {selected, _, MsgSeqIds} =
                odbc:sql_query(Conn, "select msg_id, seq_id from ledger where queue = " ++
                               QStr ++ " and seq_id >= " ++ ReadSeqId),
            MsgSeqIds2 = lists:map(
                           fun ({MsgIdStr, SeqIdStr}) ->
                                   { binary_to_term(hex_string_to_binary(MsgIdStr)),
                                     list_to_integer(SeqIdStr) }
                           end, MsgSeqIds),
            {ok, State2} = remove_messages(Q, MsgSeqIds2, true, State),
            {ok, length(MsgSeqIds2), State2}
    end.
