%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(credit_flow).

%% Credit flow is controlled by a credit specification - a
%% {InitialCredit, MoreCreditAfter} tuple. For the message sender,
%% credit starts at InitialCredit and is decremented with every
%% message sent. The message receiver grants more credit to the sender
%% by sending it a {bump_credit, ...} control message after receiving
%% MoreCreditAfter messages. The sender should pass this message in to
%% handle_bump_msg/1. The sender should block when it goes below 0
%% (check by invoking blocked/0). If a process is both a sender and a
%% receiver it will not grant any more credit to its senders when it
%% is itself blocked - thus the only processes that need to check
%% blocked/0 are ones that read from network sockets.
%%
%% Credit flows left to right when process send messages down the
%% chain, starting at the rabbit_reader, ending at the msg_store:
%%  reader -> channel -> queue_process -> msg_store.
%%
%% If the message store has a back log, then it will block the
%% queue_process, which will block the channel, and finally the reader
%% will be blocked, throttling down publishers.
%%
%% Once a process is unblocked, it will grant credits up the chain,
%% possibly unblocking other processes:
%% reader <--grant channel <--grant queue_process <--grant msg_store.
%%
%% Grepping the project files for `credit_flow` will reveal the places
%% where this module is currently used, with extra comments on what's
%% going on at each instance. Note that credit flow between mirrors
%% synchronization has not been documented, since this doesn't affect
%% client publishes.

-define(DEFAULT_INITIAL_CREDIT, 200).
-define(DEFAULT_MORE_CREDIT_AFTER, 100).

-define(DEFAULT_CREDIT,
        case get(credit_flow_default_credit) of
            undefined ->
                Val = rabbit_misc:get_env(rabbit, credit_flow_default_credit,
                                           {?DEFAULT_INITIAL_CREDIT,
                                            ?DEFAULT_MORE_CREDIT_AFTER}),
                put(credit_flow_default_credit, Val),
                Val;
            Val       -> Val
        end).

-export([send/1, send/2, ack/1, ack/2, handle_bump_msg/1, blocked/0, state/0]).
-export([peer_down/1]).
-export([block/1, unblock/1]).

%%----------------------------------------------------------------------------

-export_type([bump_msg/0]).

-opaque(bump_msg() :: {pid(), non_neg_integer()}).
-type(credit_spec() :: {non_neg_integer(), non_neg_integer()}).

-spec send
        (pid()) -> 'ok';
        (credit_spec()) -> 'ok'.
-spec ack(pid()) -> 'ok'.
-spec ack(pid(), credit_spec()) -> 'ok'.
-spec handle_bump_msg(bump_msg()) -> 'ok'.
-spec blocked() -> boolean().
-spec peer_down(pid()) -> 'ok'.

%%----------------------------------------------------------------------------

%% process dict update macro - eliminates the performance-hurting
%% closure creation a HOF would introduce
-define(UPDATE(Key, Default, Var, Expr),
        begin
            %% We deliberately allow Var to escape from the case here
            %% to be used in Expr. Any temporary var we introduced
            %% would also escape, and might conflict.
            Var = case get(Key) of
                undefined -> Default;
                V         -> V
            end,
            put(Key, Expr)
        end).

%% If current process was blocked by credit flow in the last
%% STATE_CHANGE_INTERVAL milliseconds, state/0 will report it as "in
%% flow".
-define(STATE_CHANGE_INTERVAL, 1000000).

-ifdef(CREDIT_FLOW_TRACING).
-define(TRACE_BLOCKED(SELF, FROM), rabbit_event:notify(credit_flow_blocked,
                                     [{process, SELF},
                                      {process_info, erlang:process_info(SELF)},
                                      {from, FROM},
                                      {from_info, erlang:process_info(FROM)},
                                      {timestamp,
                                       os:system_time(
                                         milliseconds)}])).
-define(TRACE_UNBLOCKED(SELF, FROM), rabbit_event:notify(credit_flow_unblocked,
                                       [{process, SELF},
                                        {from, FROM},
                                        {timestamp,
                                         os:system_time(
                                           milliseconds)}])).
-else.
-define(TRACE_BLOCKED(SELF, FROM), ok).
-define(TRACE_UNBLOCKED(SELF, FROM), ok).
-endif.

%%----------------------------------------------------------------------------

%% There are two "flows" here; of messages and of credit, going in
%% opposite directions. The variable names "From" and "To" refer to
%% the flow of credit, but the function names refer to the flow of
%% messages. This is the clearest I can make it (since the function
%% names form the API and want to make sense externally, while the
%% variable names are used in credit bookkeeping and want to make
%% sense internally).

%% For any given pair of processes, ack/2 and send/2 must always be
%% called with the same credit_spec().

send(From) -> send(From, ?DEFAULT_CREDIT).

send(From, {InitialCredit, _MoreCreditAfter}) ->
    ?UPDATE({credit_from, From}, InitialCredit, C,
            if C == 1 -> block(From),
                         0;
               true   -> C - 1
            end).

ack(To) -> ack(To, ?DEFAULT_CREDIT).

ack(To, {_InitialCredit, MoreCreditAfter}) ->
    ?UPDATE({credit_to, To}, MoreCreditAfter, C,
            if C == 1 -> grant(To, MoreCreditAfter),
                         MoreCreditAfter;
               true   -> C - 1
            end).

handle_bump_msg({From, MoreCredit}) ->
    ?UPDATE({credit_from, From}, 0, C,
            if C =< 0 andalso C + MoreCredit > 0 -> unblock(From),
                                                    C + MoreCredit;
               true                              -> C + MoreCredit
            end).

blocked() -> case get(credit_blocked) of
                 undefined -> false;
                 []        -> false;
                 _         -> true
             end.

state() -> case blocked() of
               true  -> flow;
               false -> case get(credit_blocked_at) of
                            undefined -> running;
                            B         -> Now = erlang:monotonic_time(),
                                         Diff = erlang:convert_time_unit(Now - B,
                                                                              native,
                                                                              micro_seconds),
                                         case Diff < ?STATE_CHANGE_INTERVAL of
                                             true  -> flow;
                                             false -> running
                                         end
                        end
           end.

peer_down(Peer) ->
    %% In theory we could also remove it from credit_deferred here, but it
    %% doesn't really matter; at some point later we will drain
    %% credit_deferred and thus send messages into the void...
    unblock(Peer),
    erase({credit_from, Peer}),
    erase({credit_to, Peer}),
    ok.

%% --------------------------------------------------------------------------

grant(To, Quantity) ->
    Msg = {bump_credit, {self(), Quantity}},
    case blocked() of
        false -> To ! Msg;
        true  -> ?UPDATE(credit_deferred, [], Deferred, [{To, Msg} | Deferred])
    end.

block(From) ->
    ?TRACE_BLOCKED(self(), From),
    case blocked() of
        false -> put(credit_blocked_at, erlang:monotonic_time());
        true  -> ok
    end,
    ?UPDATE(credit_blocked, [], Blocks, [From | Blocks]).

unblock(From) ->
    ?TRACE_UNBLOCKED(self(), From),
    ?UPDATE(credit_blocked, [], Blocks, Blocks -- [From]),
    case blocked() of
        false -> case erase(credit_deferred) of
                     undefined -> ok;
                     Credits   -> _ = [To ! Msg || {To, Msg} <- Credits],
                                  ok
                 end;
        true  -> ok
    end.
