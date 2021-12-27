-module(rabbit_fifo_dlx).

-include("rabbit_fifo_dlx.hrl").
-include("rabbit_fifo.hrl").
-compile({no_auto_import, [apply/3]}).

-export([
         %% rabbit_fifo_dlx_client
         make_checkout/2,
         make_settle/1,
         %% rabbit_fifo delegating DLX handling to this module
         init/0,
         apply/3,
         discard/3,
         overview/1,
         checkout/1,
         state_enter/2,
         handle_aux/4,
         purge/1,
         dehydrate/1,
         normalize/1,
         stat/1,
         update_config/2
        ]).

-record(checkout,{
          consumer :: pid(),
          prefetch :: non_neg_integer()
         }).
-record(settle, {msg_ids :: [msg_id()]}).
-type command() :: #checkout{} | #settle{}.
-type protocol() :: {dlx, command()}.
-type state() :: #?MODULE{}.
-export_type([state/0,
              protocol/0,
              reason/0]).

-spec init() -> state().
init() ->
    #?MODULE{}.

make_checkout(Pid, NumUnsettled) ->
    {dlx, #checkout{consumer = Pid,
                    prefetch = NumUnsettled
                   }}.

make_settle(MessageIds) when is_list(MessageIds) ->
    {dlx, #settle{msg_ids = MessageIds}}.

-spec overview(rabbit_fifo:state()) -> map().
overview(#rabbit_fifo{dlx = #?MODULE{consumer = undefined,
                                     msg_bytes = MsgBytes,
                                     msg_bytes_checkout = 0,
                                     discards = Discards}}) ->
    overview0(Discards, #{}, MsgBytes, 0);
overview(#rabbit_fifo{dlx = #?MODULE{consumer = #dlx_consumer{checked_out = Checked},
                                     msg_bytes = MsgBytes,
                                     msg_bytes_checkout = MsgBytesCheckout,
                                     discards = Discards}}) ->
    overview0(Discards, Checked, MsgBytes, MsgBytesCheckout).

overview0(Discards, Checked, MsgBytes, MsgBytesCheckout) ->
    #{num_discarded => lqueue:len(Discards),
      num_discard_checked_out => map_size(Checked),
      discard_message_bytes => MsgBytes,
      discard_checkout_message_bytes => MsgBytesCheckout}.

-spec stat(rabbit_fifo:state()) ->
    {Num :: non_neg_integer(), Bytes :: non_neg_integer()}.
stat(#rabbit_fifo{dlx = #?MODULE{consumer = Con,
                                 discards = Discards,
                                 msg_bytes = MsgBytes,
                                 msg_bytes_checkout = MsgBytesCheckout}}) ->
    Num0 = lqueue:len(Discards),
    Num = case Con of
              undefined ->
                  Num0;
              #dlx_consumer{checked_out = Checked} ->
                  Num0 + map_size(Checked)
          end,
    Bytes = MsgBytes + MsgBytesCheckout,
    {Num, Bytes}.

-spec apply(ra_machine:command_meta_data(), rabbit_fifo:command(), rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects()}.
apply(Meta, {dlx, #checkout{consumer = Pid,
                            prefetch = Prefetch}},
      #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                   dlx = #?MODULE{consumer = undefined} = DlxState0} = State0) ->
    DlxState = DlxState0#?MODULE{consumer = #dlx_consumer{pid = Pid,
                                                          prefetch = Prefetch}},
    State = set(State0, DlxState),
    rabbit_fifo:checkout(Meta, State0, State, [], false);
apply(Meta, {dlx, #checkout{consumer = ConsumerPid,
                            prefetch = Prefetch}},
      #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                   dlx = #?MODULE{consumer = #dlx_consumer{checked_out = CheckedOutOldConsumer},
                                  discards = Discards0,
                                  msg_bytes = Bytes,
                                  msg_bytes_checkout = BytesCheckout} = DlxState0} = State0) ->
    %% Since we allow only a single consumer, the new consumer replaces the old consumer.
    %% All checked out messages to the old consumer need to be returned to the discards queue
    %% such that these messages can be (eventually) re-delivered to the new consumer.
    %% When inserting back into the discards queue, we respect the original order in which messages
    %% were discarded.
    Checked0 = maps:to_list(CheckedOutOldConsumer),
    Checked1 = lists:keysort(1, Checked0),
    {Discards, BytesMoved} = lists:foldr(
                               fun({_Id, {_Reason, IdxMsg} = Msg}, {D, B}) ->
                                       {lqueue:in_r(Msg, D), B + size_in_bytes(IdxMsg)}
                               end, {Discards0, 0}, Checked1),
    DlxState = DlxState0#?MODULE{consumer = #dlx_consumer{pid = ConsumerPid,
                                                          prefetch = Prefetch},
                                 discards = Discards,
                                 msg_bytes = Bytes + BytesMoved,
                                 msg_bytes_checkout = BytesCheckout - BytesMoved},
    State = set(State0, DlxState),
    rabbit_fifo:checkout(Meta, State0, State, [], false);
apply(#{index := IncomingRaftIdx} = Meta, {dlx, #settle{msg_ids = MsgIds}},
      #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                   dlx = #?MODULE{consumer = #dlx_consumer{checked_out = Checked} = C,
                                  msg_bytes_checkout = BytesCheckout} = DlxState0} = State0) ->
    Acked = maps:with(MsgIds, Checked),
    AckedRsnMsgs = maps:values(Acked),
    AckedMsgs = lists:map(fun({_Reason, Msg}) -> Msg end, AckedRsnMsgs),
    AckedBytes = lists:foldl(fun(Msg, Bytes) ->
                                     Bytes + size_in_bytes(Msg)
                             end, 0, AckedMsgs),
    Unacked = maps:without(MsgIds, Checked),
    DlxState = DlxState0#?MODULE{consumer = C#dlx_consumer{checked_out = Unacked},
                                 msg_bytes_checkout = BytesCheckout - AckedBytes},
    State1 = set(State0, DlxState),
    Total = rabbit_fifo:query_messages_total(State0) - length(AckedMsgs),
    State2 = rabbit_fifo:subtract_in_memory(AckedMsgs, State1),
    State3 = State2#rabbit_fifo{messages_total = Total},
    State4 = rabbit_fifo:delete_indexes(AckedMsgs, State3),
    {State, ok, Effects} = rabbit_fifo:checkout(Meta, State0, State4, [], false),
    rabbit_fifo:update_smallest_raft_index(IncomingRaftIdx, State, Effects);
apply(_, Cmd, #rabbit_fifo{cfg = #cfg{dead_letter_handler = DLH}} = State) ->
    rabbit_log:debug("Ignoring command ~p for dead_letter_handler ~p", Cmd, DLH),
    {State, []}.

-spec discard([msg()], rabbit_dead_letter:reason(), rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects(), Delete :: boolean()}.
discard(_, _, #rabbit_fifo{cfg = #cfg{dead_letter_handler = undefined}} = State) ->
    {State, [], true};
discard(Msgs, Reason,
        #rabbit_fifo{cfg = #cfg{dead_letter_handler = {at_most_once, {Mod, Fun, Args}}}} = State) ->
    RaftIdxs = lists:filtermap(
                 fun (?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))) ->
                         {true, RaftIdx};
                     (_IgnorePrefixMessage) ->
                         false
                 end, Msgs),
    Effect = {log, RaftIdxs,
              fun (Log) ->
                      Lookup = maps:from_list(lists:zip(RaftIdxs, Log)),
                      DeadLetters = lists:filtermap(
                                      fun (?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))) ->
                                              {enqueue, _, _, Msg} = maps:get(RaftIdx, Lookup),
                                              {true, {Reason, Msg}};
                                          (?INDEX_MSG(_, ?MSG(_Header, Msg))) ->
                                              {true, {Reason, Msg}};
                                          (_IgnorePrefixMessage) ->
                                              false
                                      end, Msgs),
                      [{mod_call, Mod, Fun, Args ++ [DeadLetters]}]
              end},
    {State, [Effect], true};
discard(Msgs, Reason,
        #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                     dlx = #?MODULE{discards = Discards0,
                                    msg_bytes = MsgBytes0} = DlxState0} = State0)
  when Reason =/= maxlen ->
    %%TODO delete delivery_count header to save space?
    %% It's not needed anymore.
    {Discards, MsgBytes} = lists:foldl(fun (Msg, {D0, B0}) ->
                                               D = lqueue:in({Reason, Msg}, D0),
                                               B = B0 + size_in_bytes(Msg),
                                               {D, B}
                                       end, {Discards0, MsgBytes0}, Msgs),
    DlxState = DlxState0#?MODULE{discards = Discards,
                                 msg_bytes = MsgBytes},
    State = set(State0, DlxState),
    {State, [], false}.

-spec checkout(rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects()}.
checkout(#rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                      dlx = #?MODULE{consumer = undefined,
                                     discards = Discards}} = State) ->
    case lqueue:is_empty(Discards) of
        true ->
            ok;
        false ->
            rabbit_log:warning("there are dead-letter messages but no dead-letter consumer")
    end,
    {State, []};
checkout(#rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                      dlx = DlxState0} = State0) ->
    {DlxState, Effects} = checkout0(checkout_one(DlxState0), {[],[]}),
    State = set(State0, DlxState),
    {State, Effects};
checkout(State) ->
    {State, []}.

checkout0({success, MsgId, {Reason, ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header))}, State}, {InMemMsgs, LogMsgs})
  when is_integer(RaftIdx) ->
    DelMsg = {RaftIdx, {Reason, MsgId, Header}},
    SendAcc = {InMemMsgs, [DelMsg|LogMsgs]},
    checkout0(checkout_one(State ), SendAcc);
checkout0({success, MsgId, {Reason, ?INDEX_MSG(Idx, ?MSG(Header, Msg))}, State}, {InMemMsgs, LogMsgs})
  when is_integer(Idx) ->
    DelMsg = {MsgId, {Reason, Header, Msg}},
    SendAcc = {[DelMsg|InMemMsgs], LogMsgs},
    checkout0(checkout_one(State), SendAcc);
checkout0({success, _MsgId, {_Reason, ?TUPLE(_, _)}, State}, SendAcc) ->
    %% This is a prefix message which means we are recovering from a snapshot.
    %% We know:
    %% 1. This message was already delivered in the past, and
    %% 2. The recovery Raft log ahead of this Raft command will defintely settle this message.
    %% Therefore, here, we just check this message out to the consumer but do not re-deliver this message
    %% so that we will end up with the correct and deterministic state once the whole recovery log replay is completed.
    checkout0(checkout_one(State), SendAcc);
checkout0(#?MODULE{consumer = #dlx_consumer{pid = Pid}} = State, SendAcc) ->
    Effects = delivery_effects(Pid, SendAcc),
    {State, Effects}.

checkout_one(#?MODULE{consumer = #dlx_consumer{checked_out = Checked,
                                               prefetch = Prefetch}} = State) when map_size(Checked) >= Prefetch ->
    State;
checkout_one(#?MODULE{consumer = #dlx_consumer{checked_out = Checked0,
                                               next_msg_id = Next} = Con0} = State0) ->
    case take_next_msg(State0) of
        {{_, Msg} = ReasonMsg, State1} ->
            Checked = maps:put(Next, ReasonMsg, Checked0),
            State2 = State1#?MODULE{consumer = Con0#dlx_consumer{checked_out = Checked,
                                                                 next_msg_id = Next + 1}},
            Bytes = size_in_bytes(Msg),
            State = add_bytes_checkout(Bytes, State2),
            {success, Next, ReasonMsg, State};
        empty ->
            State0
    end.

take_next_msg(#?MODULE{discards = Discards0} = State) ->
    case lqueue:out(Discards0) of
        {empty, _} ->
            empty;
        {{value, ReasonMsg}, Discards} ->
            {ReasonMsg, State#?MODULE{discards = Discards}}
    end.

add_bytes_checkout(Size, #?MODULE{msg_bytes = Bytes,
                                  msg_bytes_checkout = BytesCheckout} = State) ->
    State#?MODULE{msg_bytes = Bytes - Size,
                  msg_bytes_checkout = BytesCheckout + Size}.

size_in_bytes(Msg) ->
    Header = rabbit_fifo:get_msg_header(Msg),
    rabbit_fifo:get_header(size, Header).

%% returns at most one delivery effect because there is only one consumer
delivery_effects(_CPid, {[], []}) ->
    [];
delivery_effects(CPid, {InMemMsgs, []}) ->
    [{send_msg, CPid, {dlx_delivery, lists:reverse(InMemMsgs)}, [ra_event]}];
delivery_effects(CPid, {InMemMsgs, IdxMsgs0}) ->
    IdxMsgs = lists:reverse(IdxMsgs0),
    {RaftIdxs, Data} = lists:unzip(IdxMsgs),
    [{log, RaftIdxs,
      fun(Log) ->
              Msgs0 = lists:zipwith(fun ({enqueue, _, _, Msg}, {Reason, MsgId, Header}) ->
                                            {MsgId, {Reason, Header, Msg}}
                                    end, Log, Data),
              Msgs = case InMemMsgs of
                         [] ->
                             Msgs0;
                         _ ->
                             lists:sort(InMemMsgs ++ Msgs0)
                     end,
              [{send_msg, CPid, {dlx_delivery, Msgs}, [ra_event]}]
      end}].

-spec state_enter(ra_server:ra_state(), rabbit_fifo:state()) ->
    ra_machine:effects().
state_enter(leader, #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once,
                                            resource = QRef},
                                 dlx = DlxState}) ->
    ensure_worker_started(QRef, DlxState),
    [];
state_enter(_, #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                            dlx = DlxState}) ->
    ensure_worker_terminated(DlxState),
    [];
state_enter(_, _) ->
    [].

ensure_worker_started(QRef, #?MODULE{consumer = undefined}) ->
    start_worker(QRef);
ensure_worker_started(QRef, #?MODULE{consumer = #dlx_consumer{pid = Pid}}) ->
    case is_local_and_alive(Pid) of
        true ->
            rabbit_log:debug("rabbit_fifo_dlx_worker ~p already started for ~s",
                             [Pid, rabbit_misc:rs(QRef)]);
        false ->
            start_worker(QRef)
    end.

%% Ensure that starting the rabbit_fifo_dlx_worker succeeds.
%% Therefore, do not use an effect.
%% Also therefore, if starting the rabbit_fifo_dlx_worker fails, let the
%% Ra server process crash in which case another Ra node will become leader.
start_worker(QRef) ->
    {ok, Pid} = supervisor:start_child(rabbit_fifo_dlx_sup, [QRef]),
    rabbit_log:debug("started rabbit_fifo_dlx_worker ~p for ~s",
                     [Pid, rabbit_misc:rs(QRef)]).

ensure_worker_terminated(#?MODULE{consumer = undefined}) ->
    ok;
ensure_worker_terminated(#?MODULE{consumer = #dlx_consumer{pid = Pid}}) ->
    case is_local_and_alive(Pid) of
        true ->
            %% Note that we can't return a mod_call effect here
            %% because mod_call is executed on the leader only.
            ok = supervisor:terminate_child(rabbit_fifo_dlx_sup, Pid),
            rabbit_log:debug("terminated rabbit_fifo_dlx_worker ~p", [Pid]);
        false ->
            ok
    end.

local_alive_consumer_pid(#?MODULE{consumer = undefined}) ->
    undefined;
local_alive_consumer_pid(#?MODULE{consumer = #dlx_consumer{pid = Pid}}) ->
    case is_local_and_alive(Pid) of
        true ->
            Pid;
        false ->
            undefined
    end.

is_local_and_alive(Pid)
  when node(Pid) =:= node() ->
    is_process_alive(Pid);
is_local_and_alive(_) ->
    false.

-spec update_config(config(), rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects()}.
update_config(#{dead_letter_handler := at_least_once},
              #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                           dlx = DlxState} = State) ->
    %% dead_letter_handler stayed at_least_once.
    %% Notify rabbit_fifo_dlx_worker about potentially updated policies.
    case local_alive_consumer_pid(DlxState) of
        undefined ->
            {State, []};
        Pid ->
            {State, [{send_msg, Pid, lookup_topology, ra_event}]}
    end;
update_config(#{dead_letter_handler := DLH},
              #rabbit_fifo{cfg = #cfg{dead_letter_handler = DLH}} = State) ->
    %% dead_letter_handler stayed same.
    {State, []};
update_config(#{dead_letter_handler := NewDLH},
              #rabbit_fifo{cfg = #cfg{dead_letter_handler = OldDLH,
                                      resource = Res}} = State0) ->
    rabbit_log:debug("Switching dead_letter_handler from ~p to ~p for ~s",
                     [OldDLH, NewDLH, rabbit_misc:rs(Res)]),
    {#rabbit_fifo{cfg = Cfg} = State1, Effects0} = switch_from(State0),
    State2 = State1#rabbit_fifo{cfg = Cfg#cfg{dead_letter_handler = NewDLH},
                                dlx = init()},
    switch_to(State2, Effects0).

-spec switch_to(rabbit_fifo:state(), ra_machine:effects()) ->
    {rabbit_fifo:state(), ra_machine:effects()}.
switch_to(#rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once}} = State,
          Effects0) ->
    %% Switch from some other strategy to at-least-once.
    %% Dlx worker needs to be started on the leader.
    %% The cleanest way to determine the Ra state of this node is delegation to handle_aux.
    Effects = [{aux, {dlx, setup}} | Effects0],
    {State, Effects};
switch_to(State, Effects) ->
    {State, Effects}.

-spec switch_from(rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects()}.
switch_from(#rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once},
                         dlx = #?MODULE{consumer = Consumer,
                                        discards = Discards} = DlxState} = State0) ->
    %% switch from at-least-once to some other strategy
    ensure_worker_terminated(DlxState),
    CheckedReasonMsgs = case Consumer of
                            #dlx_consumer{checked_out = Checked}
                              when is_map(Checked) ->
                                maps:values(Checked);
                            _ -> []
                        end,
    DiscardReasonMsgs = lqueue:to_list(Discards),
    {_, Msgs} = lists:unzip(CheckedReasonMsgs ++ DiscardReasonMsgs),
    Len = length(Msgs),
    Total = rabbit_fifo:query_messages_total(State0),
    State1 = State0#rabbit_fifo{messages_total = Total - Len},
    State2 = rabbit_fifo:delete_indexes(Msgs, State1),
    State = rabbit_fifo:subtract_in_memory(Msgs, State2),
    rabbit_log:debug("Deleted ~b dead-lettered messages", [Len]),
    {State, []};
switch_from(State) ->
    {State, []}.

-spec handle_aux(ra_server:ra_state(), Cmd :: term(), term(), rabbit_fifo:state()) ->
    term().
handle_aux(leader, setup, Aux,
           #rabbit_fifo{cfg = #cfg{dead_letter_handler = at_least_once,
                                   resource = QRef},
                        dlx = DlxState}) ->
    ensure_worker_started(QRef, DlxState),
    Aux;
handle_aux(_, _, Aux, _) ->
    Aux.

-spec purge(rabbit_fifo:state()) ->
    {rabbit_fifo:state(), [msg()]}.
purge(#rabbit_fifo{dlx = #?MODULE{consumer = Con0,
                                  discards = Discards} = DlxState0} = State0) ->
    {Con, CheckedMsgs} = case Con0 of
                             #dlx_consumer{checked_out = Checked}
                               when is_map(Checked) ->
                                 L = maps:to_list(Checked),
                                 {_, CheckedReasonMsgs} = lists:unzip(L),
                                 {_, Msgs} = lists:unzip(CheckedReasonMsgs),
                                 C = Con0#dlx_consumer{checked_out = #{}},
                                 {C, Msgs};
                             _ ->
                                 {Con0, []}
                         end,
    DiscardReasonMsgs = lqueue:to_list(Discards),
    {_, DiscardMsgs} = lists:unzip(DiscardReasonMsgs),
    PurgedMsgs = CheckedMsgs ++ DiscardMsgs,
    DlxState = DlxState0#?MODULE{consumer = Con,
                                 discards = lqueue:new(),
                                 msg_bytes = 0,
                                 msg_bytes_checkout = 0
                                },
    State = set(State0, DlxState),
    {State, PurgedMsgs}.

-spec dehydrate(rabbit_fifo:state()) ->
    rabbit_fifo:state().
dehydrate(#rabbit_fifo{dlx = #?MODULE{discards = Discards,
                                      consumer = Con} = DlxState} = State) ->
    set(State, DlxState#?MODULE{discards = dehydrate_messages(Discards),
                                consumer = dehydrate_consumer(Con)}).

dehydrate_messages(Discards) ->
    L0 = lqueue:to_list(Discards),
    L1 = lists:map(fun({_Reason, Msg}) ->
                           {?NIL, rabbit_fifo:dehydrate_message(Msg)}
                   end, L0),
    lqueue:from_list(L1).

dehydrate_consumer(#dlx_consumer{checked_out = Checked0} = Con) ->
    Checked = maps:map(fun (_, {_, Msg}) ->
                               {?NIL, rabbit_fifo:dehydrate_message(Msg)}
                       end, Checked0),
    Con#dlx_consumer{checked_out = Checked};
dehydrate_consumer(undefined) ->
    undefined.

-spec normalize(rabbit_fifo:state()) ->
    rabbit_fifo:state().
normalize(#rabbit_fifo{dlx = #?MODULE{discards = Discards} = DlxState} = State) ->
    set(State, DlxState#?MODULE{discards = lqueue:from_list(lqueue:to_list(Discards))}).

set(State, #?MODULE{} = DlxState) ->
    State#rabbit_fifo{dlx = DlxState}.
