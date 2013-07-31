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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
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

-define(DEFAULT_CREDIT, {200, 50}).

-export([send/1, send/2, ack/1, ack/2, handle_bump_msg/1, blocked/0]).
-export([peer_down/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-opaque(bump_msg() :: {pid(), non_neg_integer()}).
-type(credit_spec() :: {non_neg_integer(), non_neg_integer()}).

-spec(send/1 :: (pid()) -> 'ok').
-spec(send/2 :: (pid(), credit_spec()) -> 'ok').
-spec(ack/1 :: (pid()) -> 'ok').
-spec(ack/2 :: (pid(), credit_spec()) -> 'ok').
-spec(handle_bump_msg/1 :: (bump_msg()) -> 'ok').
-spec(blocked/0 :: () -> boolean()).
-spec(peer_down/1 :: (pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

%% process dict update macro - eliminates the performance-hurting
%% closure creation a HOF would introduce
-define(UPDATE(Key, Default, Var, Expr),
        begin
            %% We deliberately allow Var to escape from the case here
            %% to be used in Expr. Any temporary var we introduced
            %% would also escape, and might conflict.
            case get(Key) of
                undefined -> Var = Default;
                Var       -> ok
            end,
            put(Key, Expr)
        end).

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

block(From) -> ?UPDATE(credit_blocked, [], Blocks, [From | Blocks]).

unblock(From) ->
    ?UPDATE(credit_blocked, [], Blocks, Blocks -- [From]),
    case blocked() of
        false -> case erase(credit_deferred) of
                     undefined -> ok;
                     Credits   -> [To ! Msg || {To, Msg} <- Credits]
                 end;
        true  -> ok
    end.
