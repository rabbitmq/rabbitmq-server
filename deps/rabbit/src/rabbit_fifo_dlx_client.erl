%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(rabbit_fifo_dlx_client).

-export([checkout/3, settle/2, handle_ra_event/3,
         overview/1]).

-record(state,{
          queue_resource :: rabbit_types:r(queue),
          leader :: ra:server_id(),
          last_msg_id :: non_neg_integer() | -1
         }).
-type state() :: #state{}.
-type action() :: {deliver, [{rabbit_amqqueue:name(),
                              MsgId :: non_neg_integer(),
                              rabbit_types:message(),
                              rabbit_dead_letter:reason()}]}.
-type actions() :: [action()].
-export_type([state/0,
              actions/0]).

-spec settle([non_neg_integer()], state()) ->
    {ok, state()}.
settle(MsgIds, #state{leader = Leader} = State)
  when is_list(MsgIds) ->
    Cmd = rabbit_fifo_dlx:make_settle(MsgIds),
    ra:pipeline_command(Leader, Cmd),
    {ok, State}.

-spec checkout(rabbit_amqqueue:name(), ra:server_id(), non_neg_integer()) ->
    {ok, state()} | {error, non_local_leader | ra_command_failed}.
checkout(QResource, Leader, NumUnsettled) ->
    Cmd = rabbit_fifo_dlx:make_checkout(self(), NumUnsettled),
    State = #state{queue_resource = QResource,
                   leader = Leader,
                   last_msg_id = -1},
    process_command(Cmd, State, 5).

process_command(_Cmd, _State, 0) ->
    {error, ra_command_failed};
process_command(Cmd, #state{leader = Leader} = State, Tries) ->
    case ra:process_command(Leader, Cmd, 60_000) of
        {ok, ok, Leader} ->
            {ok, State#state{leader = Leader}};
        {ok, ok, NonLocalLeader} ->
            rabbit_log:warning("Failed to process command ~tp on quorum queue leader ~tp because actual leader is ~tp.",
                               [Cmd, Leader, NonLocalLeader]),
            {error, non_local_leader};
        Err ->
            rabbit_log:warning("Failed to process command ~tp on quorum queue leader ~tp: ~tp~n"
                               "Trying ~b more time(s)...",
                               [Cmd, Leader, Err, Tries]),
            process_command(Cmd, State, Tries - 1)
    end.

-spec handle_ra_event(ra:server_id(), term(), state()) ->
    {ok, state(), actions()}.
handle_ra_event(Leader, {machine, {dlx_delivery, _} = Del}, #state{leader = Leader} = State) ->
    handle_delivery(Del, State);
handle_ra_event(From, Evt, State) ->
    rabbit_log:debug("Ignoring ra event ~tp from ~tp", [Evt, From]),
    {ok, State, []}.

handle_delivery({dlx_delivery, [{FstId, _} | _] = IdMsgs},
                #state{queue_resource = QRes,
                       last_msg_id = Prev} = State0) ->
    %% Assert that messages get delivered in order since deliveries are node local.
    %% (In contrast to rabbit_fifo_client, we expect neither duplicate nor missing messages.)
    %% Let it crash if this assertion is wrong.
    FstId = Prev + 1,
    %% Format as a deliver action.
    Del = {deliver, transform_msgs(QRes, IdMsgs)},
    {LastId, _} = lists:last(IdMsgs),
    State = State0#state{last_msg_id = LastId},
    {ok, State, [Del]}.

transform_msgs(QRes, Msgs) ->
    lists:map(
      fun({MsgId, {Reason, Msg}}) ->
              {QRes, MsgId, Msg, Reason}
      end, Msgs).

-spec overview(state()) -> map().
overview(#state{leader = Leader,
                last_msg_id = LastMsgId}) ->
    #{leader => Leader,
      last_msg_id => LastMsgId}.
