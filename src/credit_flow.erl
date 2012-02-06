%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(credit_flow).

%% Credit starts at MaxCredit and goes down. Both sides keep
%% track. When the receiver goes below MoreCreditAt it issues more
%% credit by sending a message to the sender. The sender should pass
%% this message in to handle_bump_msg/1. The sender should block when
%% it goes below 0 (check by invoking blocked/0). If a process is both
%% a sender and a receiver it will not grant any more credit to its
%% senders when it is itself blocked - thus the only processes that
%% need to check blocked/0 are ones that read from network sockets.

-define(DEFAULT_CREDIT, {200, 150}).

-export([ack/1, ack/2, handle_bump_msg/1, blocked/0, send/1, send/2]).
-export([peer_down/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-opaque(bump_msg() :: {pid(), non_neg_integer()}).
-opaque(credit_spec() :: {non_neg_integer(), non_neg_integer()}).

-spec(ack/1 :: (pid()) -> 'ok').
-spec(ack/2 :: (pid(), credit_spec()) -> 'ok').
-spec(handle_bump_msg/1 :: (bump_msg()) -> 'ok').
-spec(blocked/0 :: () -> boolean()).
-spec(send/1 :: (pid()) -> 'ok').
-spec(send/2 :: (pid(), credit_spec()) -> 'ok').
-spec(peer_down/1 :: (pid()) -> 'ok').

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

ack(To) -> ack(To, ?DEFAULT_CREDIT).

ack(To, {MaxCredit, MoreCreditAt}) ->
    MoreCreditAt1 = MoreCreditAt + 1,
    Credit = case get({credit_to, To}, MaxCredit) of
                 MoreCreditAt1 -> grant(To, MaxCredit - MoreCreditAt),
                                  MaxCredit;
                 C             -> C - 1
             end,
    put({credit_to, To}, Credit).

handle_bump_msg({From, MoreCredit}) ->
    Credit = get({credit_from, From}, 0) + MoreCredit,
    put({credit_from, From}, Credit),
    case Credit > 0 of
        true  -> unblock(From),
                 ok;
        false -> ok
    end.

blocked() -> get(credit_blocked, []) =/= [].

send(From) -> send(From, ?DEFAULT_CREDIT).

send(From, {MaxCredit, _MoreCreditAt}) ->
    Credit = get({credit_from, From}, MaxCredit) - 1,
    case Credit of
        0 -> block(From);
        _ -> ok
    end,
    put({credit_from, From}, Credit).

peer_down(Peer) ->
    %% In theory we could also remove it from credit_deferred here, but it
    %% doesn't really matter; at some point later we will drain
    %% credit_deferred and thus send messages into the void...
    unblock(Peer),
    erase({credit_from, Peer}),
    erase({credit_to, Peer}).

%% --------------------------------------------------------------------------

grant(To, Quantity) ->
    Msg = {bump_credit, {self(), Quantity}},
    case blocked() of
        false -> To ! Msg;
        true  -> Deferred = get(credit_deferred, []),
                 put(credit_deferred, [{To, Msg} | Deferred])
    end.

block(From) -> put(credit_blocked, [From | get(credit_blocked, [])]).

unblock(From) ->
    NewBlocks = get(credit_blocked, []) -- [From],
    put(credit_blocked, NewBlocks),
    case NewBlocks of
        [] -> [To ! Msg || {To, Msg} <- get(credit_deferred, [])],
              erase(credit_deferred);
        _  -> ok
    end.

get(Key, Default) ->
    case get(Key) of
        undefined -> Default;
        Value     -> Value
    end.
