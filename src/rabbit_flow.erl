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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_flow).

-define(MAX_CREDIT, 100).
-define(MORE_CREDIT_AT, 50).

-export([ack/1, bump/1, blocked/0, send/1]).

%% There are two "flows" here; of messages and of credit, going in
%% opposite directions. The variable names "From" and "To" refer to
%% the flow of credit, but the function names refer to the flow of
%% messages. This is the clearest I can make it (since the function
%% names form the API and want to make sense externally, while the
%% variable names are used in credit bookkeeping and want to make
%% sense internally).

ack(To) ->
    Credit =
        case get({credit_to, To}) of
            undefined           -> ?MAX_CREDIT;
            ?MORE_CREDIT_AT + 1 -> grant(To, ?MAX_CREDIT - ?MORE_CREDIT_AT),
                                   ?MAX_CREDIT;
            C                   -> C - 1
        end,
    put({credit_to, To}, Credit).

bump({From, MoreCredit}) ->
    Credit = case get({credit_from, From}) of
                 undefined -> MoreCredit;
                 C         -> C + MoreCredit
             end,
    put({credit_from, From}, Credit),
    case Credit > 0 of
        true  -> unblock(),
                 false;
        false -> true
    end.

%% TODO we assume only one From can block at once. Is this true?
blocked() ->
    get(credit_blocked) =:= true.

send(From) ->
    Credit = case get({credit_from, From}) of
                 undefined -> ?MAX_CREDIT;
                 C         -> C
             end - 1,
    case Credit of
        0 -> put(credit_blocked, true);
        _ -> ok
    end,
    put({credit_from, From}, Credit).

%% --------------------------------------------------------------------------

grant(To, Quantity) ->
    Msg = {bump_credit, {self(), Quantity}},
    case blocked() of
        false -> To ! Msg;
        true  -> Deferred = case get(credit_deferred) of
                                undefined -> [];
                                L         -> L
                            end,
                 put(credit_deferred, [{To, Msg} | Deferred])
    end.

unblock() ->
    erase(credit_blocked),
    case get(credit_deferred) of
        undefined -> ok;
        Deferred  -> [To ! Msg || {To, Msg} <- Deferred],
                     erase(credit_deferred)
    end.
