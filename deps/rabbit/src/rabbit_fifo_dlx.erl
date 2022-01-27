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
         apply/4,
         discard/4,
         overview/1,
         checkout/2,
         state_enter/4,
         handle_aux/6,
         purge/1,
         dehydrate/1,
         normalize/1,
         stat/1,
         update_config/4,
         smallest_raft_index/1
        ]).

-record(checkout,{
          consumer :: pid(),
          prefetch :: non_neg_integer()
         }).
-record(settle, {msg_ids :: [msg_id()]}).
-type protocol() :: {dlx, #checkout{} | #settle{}}.
-type state() :: #?MODULE{}.
-export_type([state/0,
              protocol/0]).

-spec init() -> state().
init() ->
    #?MODULE{}.

make_checkout(Pid, NumUnsettled) ->
    {dlx, #checkout{consumer = Pid,
                    prefetch = NumUnsettled
                   }}.

make_settle(MessageIds) when is_list(MessageIds) ->
    {dlx, #settle{msg_ids = MessageIds}}.

-spec overview(state()) -> map().
overview(#?MODULE{consumer = undefined,
                  msg_bytes = MsgBytes,
                  msg_bytes_checkout = 0,
                  discards = Discards}) ->
    overview0(Discards, #{}, MsgBytes, 0);
overview(#?MODULE{consumer = #dlx_consumer{checked_out = Checked},
                  msg_bytes = MsgBytes,
                  msg_bytes_checkout = MsgBytesCheckout,
                  discards = Discards}) ->
    overview0(Discards, Checked, MsgBytes, MsgBytesCheckout).

overview0(Discards, Checked, MsgBytes, MsgBytesCheckout) ->
    #{num_discarded => lqueue:len(Discards),
      num_discard_checked_out => maps:size(Checked),
      discard_message_bytes => MsgBytes,
      discard_checkout_message_bytes => MsgBytesCheckout}.

-spec stat(state()) ->
    {Num :: non_neg_integer(), Bytes :: non_neg_integer()}.
stat(#?MODULE{consumer = Con,
              discards = Discards,
              msg_bytes = MsgBytes,
              msg_bytes_checkout = MsgBytesCheckout}) ->
    Num0 = lqueue:len(Discards),
    Num = case Con of
              undefined ->
                  Num0;
              #dlx_consumer{checked_out = Checked} ->
                  %% O(1) because Erlang maps maintain their own size
                  Num0 + maps:size(Checked)
          end,
    Bytes = MsgBytes + MsgBytesCheckout,
    {Num, Bytes}.

-spec apply(ra_machine:command_meta_data(), protocol(), dead_letter_handler(), state()) ->
    {state(), ra_machine:effects()}.
apply(_Meta, {dlx, #settle{msg_ids = MsgIds}}, at_least_once,
      #?MODULE{consumer = #dlx_consumer{checked_out = Checked0}} = State0) ->
    Acked = maps:with(MsgIds, Checked0),
    State = maps:fold(fun(MsgId, {_Rsn,?INDEX_MSG(Idx, ?DISK_MSG(_)) = Msg},
                          #?MODULE{consumer = #dlx_consumer{checked_out = Checked} = C,
                                   msg_bytes_checkout = BytesCheckout,
                                   ra_indexes = Indexes0} = S) ->
                      Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
                      S#?MODULE{consumer = C#dlx_consumer{checked_out =
                                                          maps:remove(MsgId, Checked)},
                                msg_bytes_checkout = BytesCheckout - size_in_bytes(Msg),
                                ra_indexes = Indexes}
                      end, State0, Acked),
    {State, [{mod_call, rabbit_global_counters, messages_dead_lettered_confirmed,
              [rabbit_quorum_queue, at_least_once, maps:size(Acked)]}]};
apply(_, {dlx, #checkout{consumer = Pid,
                         prefetch = Prefetch}},
      at_least_once,
      #?MODULE{consumer = undefined} = State0) ->
    State = State0#?MODULE{consumer = #dlx_consumer{pid = Pid,
                                                    prefetch = Prefetch}},
    {State, []};
apply(_, {dlx, #checkout{consumer = ConsumerPid,
                         prefetch = Prefetch}},
      at_least_once,
      #?MODULE{consumer = #dlx_consumer{checked_out = CheckedOutOldConsumer},
               discards = Discards0,
               msg_bytes = Bytes,
               msg_bytes_checkout = BytesCheckout} = State0) ->
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
    State = State0#?MODULE{consumer = #dlx_consumer{pid = ConsumerPid,
                                                    prefetch = Prefetch},
                           discards = Discards,
                           msg_bytes = Bytes + BytesMoved,
                           msg_bytes_checkout = BytesCheckout - BytesMoved},
    {State, []};
apply(_, Cmd, DLH, State) ->
    rabbit_log:debug("Ignoring command ~p for dead_letter_handler ~p", [Cmd, DLH]),
    {State, []}.

-spec discard([indexed_msg()], rabbit_dead_letter:reason(),
              dead_letter_handler(), state()) ->
    {state(), ra_machine:effects()}.
discard(Msgs, Reason, undefined, State) ->
    {State, [{mod_call, rabbit_global_counters, messages_dead_lettered,
              [Reason, rabbit_quorum_queue, disabled, length(Msgs)]}]};
discard(Msgs0, Reason, {at_most_once, {Mod, Fun, Args}}, State) ->
    RaftIdxs = lists:filtermap(
                 fun (?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))) ->
                         {true, RaftIdx};
                     (_IgnorePrefixMessage) ->
                         false
                 end, Msgs0),
    Effect = {log, RaftIdxs,
              fun (Log) ->
                      Lookup = maps:from_list(lists:zip(RaftIdxs, Log)),
                      Msgs = lists:filtermap(
                               fun (?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))) ->
                                       {enqueue, _, _, Msg} = maps:get(RaftIdx, Lookup),
                                       {true, Msg};
                                   (_IgnorePrefixMessage) ->
                                       false
                               end, Msgs0),
                      [{mod_call, Mod, Fun, Args ++ [Reason, Msgs]}]
              end},
    {State, [Effect]};
discard(Msgs, Reason, at_least_once, State0)
  when Reason =/= maxlen ->
    %%TODO delete delivery_count header to save space? It's not needed anymore.
    State = lists:foldl(fun (?INDEX_MSG(Idx, _) = Msg,
                             #?MODULE{discards = D0,
                                      msg_bytes = B0,
                                      ra_indexes = I0} = S0) ->
                                D = lqueue:in({Reason, Msg}, D0),
                                B = B0 + size_in_bytes(Msg),
                                I = rabbit_fifo_index:append(Idx, I0),
                                S0#?MODULE{discards = D,
                                           msg_bytes = B,
                                           ra_indexes = I}
                        end, State0, Msgs),
    {State, [{mod_call, rabbit_global_counters, messages_dead_lettered,
              [Reason, rabbit_quorum_queue, at_least_once, length(Msgs)]}]}.

-spec checkout(dead_letter_handler(), state()) ->
    {state(), ra_machine:effects()}.
checkout(at_least_once, #?MODULE{consumer = #dlx_consumer{}} = State) ->
    checkout0(checkout_one(State), {[],[]});
checkout(_, State) ->
    {State, []}.

checkout0({success, MsgId, {Reason, ?INDEX_MSG(Idx, ?DISK_MSG(Header))}, State}, {InMemMsgs, LogMsgs})
  when is_integer(Idx) ->
    DelMsg = {Idx, {Reason, MsgId, Header}},
    SendAcc = {InMemMsgs, [DelMsg|LogMsgs]},
    checkout0(checkout_one(State), SendAcc);
% checkout0({success, MsgId, {Reason, ?INDEX_MSG(Idx, ?MSG(Header, Msg))}, State}, {InMemMsgs, LogMsgs})
%   when is_integer(Idx) ->
%     DelMsg = {MsgId, {Reason, Header, Msg}},
%     SendAcc = {[DelMsg|InMemMsgs], LogMsgs},
%     checkout0(checkout_one(State), SendAcc);
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
                                               prefetch = Prefetch}} = State)
  when map_size(Checked) >= Prefetch ->
    State;
checkout_one(#?MODULE{discards = Discards0,
                      msg_bytes = Bytes,
                      msg_bytes_checkout = BytesCheckout,
                      consumer = #dlx_consumer{checked_out = Checked0,
                                               next_msg_id = Next} = Con0} = State0) ->
    case lqueue:out(Discards0) of
        {{value, {_, Msg} = ReasonMsg}, Discards} ->
            Checked = maps:put(Next, ReasonMsg, Checked0),
            Size = size_in_bytes(Msg),
            State = State0#?MODULE{discards = Discards,
                                   msg_bytes = Bytes - Size,
                                   msg_bytes_checkout = BytesCheckout + Size,
                                   consumer = Con0#dlx_consumer{checked_out = Checked,
                                                                next_msg_id = Next + 1}},
            {success, Next, ReasonMsg, State};
        {empty, _} ->
            State0
    end.

size_in_bytes(?INDEX_MSG(_Idx, ?DISK_MSG(Header))) ->
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

-spec state_enter(ra_server:ra_state() | eol, rabbit_types:r('queue'), dead_letter_handler(), state()) ->
    ra_machine:effects().
state_enter(leader, QRes, at_least_once, State) ->
    ensure_worker_started(QRes, State),
    [];
state_enter(_, _, at_least_once, State) ->
    ensure_worker_terminated(State),
    [];
state_enter(_, _, _, _) ->
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

-spec update_config(Old :: dead_letter_handler(), New :: dead_letter_handler(),
                    rabbit_types:r('queue'), state()) ->
    {state(), ra_machine:effects()}.
update_config(at_least_once, at_least_once, _, State) ->
    case local_alive_consumer_pid(State) of
        undefined ->
            {State, []};
        Pid ->
            %% Notify rabbit_fifo_dlx_worker about potentially updated policies.
            {State, [{send_msg, Pid, lookup_topology, ra_event}]}
    end;
update_config(SameDLH, SameDLH, _, State) ->
    {State, []};
update_config(OldDLH, NewDLH, QRes, State0) ->
    LogOnLeader = {mod_call, rabbit_log, debug,
                   ["Switching dead_letter_handler from ~p to ~p for ~s",
                    [OldDLH, NewDLH, rabbit_misc:rs(QRes)]]},
    {State1, Effects0} = switch_from(OldDLH, QRes, State0),
    {State, Effects} = switch_to(NewDLH, State1, Effects0),
    {State, [LogOnLeader|Effects]}.

-spec switch_from(Old :: dead_letter_handler(), rabbit_types:r('queue'), state()) ->
    {state(), ra_machine:effects()}.
switch_from(at_least_once, QRes, State) ->
    %% Switch from at-least-once to some other strategy.
    ensure_worker_terminated(State),
    {Num, Bytes} = stat(State),
    %% Log only on leader.
    {init(), [{mod_call, rabbit_log, info,
               ["Deleted ~b dead-lettered messages (with total messages size of ~b bytes) in ~s",
                [Num, Bytes, rabbit_misc:rs(QRes)]]}]};
switch_from(_, _, State) ->
    {State, []}.

-spec switch_to(New :: dead_letter_handler(), state(), ra_machine:effects()) ->
    {state(), ra_machine:effects()}.
switch_to(at_least_once, _, Effects) ->
    %% Switch from some other strategy to at-least-once.
    %% Dlx worker needs to be started on the leader.
    %% The cleanest way to determine the Ra state of this node is delegation to handle_aux.
    {init(), [{aux, {dlx, setup}} | Effects]};
switch_to(_, State, Effects) ->
    {State, Effects}.

-spec handle_aux(ra_server:ra_state(), Cmd :: term(), Aux :: term(),
                 rabbit_types:r('queue'), dead_letter_handler(), state()) ->
    term().
handle_aux(leader, {dlx, setup}, Aux, QRes, at_least_once, State) ->
    ensure_worker_started(QRes, State),
    Aux;
handle_aux(_, _, Aux, _, _, _) ->
    Aux.

-spec purge(state()) ->
    state().
purge(#?MODULE{consumer = Consumer0} = State) ->
    Consumer = case Consumer0 of
                   undefined ->
                       undefined;
                   #dlx_consumer{} ->
                       Consumer0#dlx_consumer{checked_out = #{}}
               end,
    State#?MODULE{discards = lqueue:new(),
                  msg_bytes = 0,
                  msg_bytes_checkout = 0,
                  consumer = Consumer,
                  ra_indexes = rabbit_fifo_index:empty()
                 }.

-spec dehydrate(state()) ->
    state().
dehydrate(#?MODULE{discards = _Discards,
                   consumer = _Con} = State) ->
    State#?MODULE{%%discards = dehydrate_messages(Discards),
                  %%consumer = dehydrate_consumer(Con),
                  ra_indexes = rabbit_fifo_index:empty()}.

% dehydrate_messages(Discards) ->
%     L0 = lqueue:to_list(Discards),
%     L1 = lists:map(fun({_Reason, Msg}) ->
%                            {?NIL, rabbit_fifo:dehydrate_message(Msg)}
%                    end, L0),
%     lqueue:from_list(L1).

% dehydrate_consumer(#dlx_consumer{checked_out = Checked0} = Con) ->
%     Checked = maps:map(fun (_, {_, Msg}) ->
%                                {?NIL, rabbit_fifo:dehydrate_message(Msg)}
%                        end, Checked0),
%     Con#dlx_consumer{checked_out = Checked};
% dehydrate_consumer(undefined) ->
%     undefined.

-spec normalize(state()) ->
    state().
normalize(#?MODULE{discards = Discards,
                   ra_indexes = Indexes} = State) ->
    State#?MODULE{discards = lqueue:from_list(lqueue:to_list(Discards)),
                  ra_indexes = rabbit_fifo_index:normalize(Indexes)}.

smallest_raft_index(#?MODULE{ra_indexes = Indexes}) ->
    rabbit_fifo_index:smallest(Indexes).
