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

-define(INITIAL_CREDIT, 100).
-define(MORE_CREDIT_RATIO, 0.8).
-define(GS2_AVG_LOW, 0.96).
-define(GS2_AVG_HI, 1.02).

-export([ack/1, bump/1, blocked/0, send/1]).

%% There are two "flows" here; of messages and of credit, going in
%% opposite directions. The variable names "From" and "To" refer to
%% the flow of credit, but the function names refer to the flow of
%% messages. This is the clearest I can make it (since the function
%% names form the API and want to make sense externally, while the
%% variable names are used in credit bookkeeping and want to make
%% sense internally).

ack(To) ->
    CreditLimit = case get({credit_limit, To}) of
                         undefined -> ?INITIAL_CREDIT;
                         C         -> C
                     end,
    put({credit_limit, To}, CreditLimit),
    MoreAt = trunc(CreditLimit * ?MORE_CREDIT_RATIO),
    %%io:format("~p -> ~p more at: ~p, currently at: ~p ~n", [self(), To, MoreAt, get({credit_to, To})]),
    Credit = case get({credit_to, To}) of
                 undefined -> CreditLimit;
                 MoreAt    -> grant(To),
                              get({credit_limit, To});
                 C2        -> C2 - 1
             end,
    put({credit_to, To}, Credit).

bump({From, MoreCredit}) ->
    Credit = case get({credit_from, From}) of
                 undefined -> MoreCredit;
                 C         -> %%io:format("~p -> ~p got ~p~n", [From, self(), C]),
                              C + MoreCredit
             end,
    put({credit_from, From}, Credit),
%%    io:format("~p -> ~p got more credit: ~p now ~p~n", [From, self(), MoreCredit, Credit]),
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
                 undefined -> ?INITIAL_CREDIT;
                 C         -> C
             end - 1,
    %%io:format("~p send credit is ~p~n", [self(), Credit]),
    case Credit of
        0 -> put(credit_blocked, true);
        _ -> ok
    end,
    put({credit_from, From}, Credit).

%% --------------------------------------------------------------------------

grant(To) ->
    OldCreditLimit = get({credit_limit, To}),
    OldMoreAt = trunc(OldCreditLimit * ?MORE_CREDIT_RATIO),
    adjust_credit(To),
    NewCreditLimit = get({credit_limit, To}),
    Quantity = NewCreditLimit - OldMoreAt + 1,
    Msg = {bump_credit, {self(), Quantity}},
    case blocked() of
        false -> %%io:format("~p -> ~p sent more credit: ~p ~n", [self(), To, {NewCreditLimit, OldMoreAt, Quantity}]),
                 To ! Msg;
        true  -> %%io:format("~p -> ~p deferred more credit: ~p ~n", [self(), To, Quantity]),
                 Deferred = case get(credit_deferred) of
                                undefined -> [];
                                L         -> L
                            end,
                 put(credit_deferred, [{To, Msg} | Deferred])
    end.

adjust_credit(To) ->
    Avg = get(gs2_avg),
    Limit0 = get({credit_limit, To}),
    Limit1 = if Avg > ?GS2_AVG_HI ->
                     L = erlang:max(trunc(Limit0 / 1.1), 10),
                     %%io:format("~p -> ~p v ~p (~p ~p)~n", [self(), To, L, Avg, queue:to_list(get(gs2_stats))]),
                     io:format("~p -> ~p v ~p (~p, ~p)~n", [self(), To, L, Avg, get(gs2_pq)]),
                     L;
                Avg < ?GS2_AVG_LOW ->
                     L = erlang:min(trunc(Limit0 * 1.1), 10000),
                     io:format("~p -> ~p ^ ~p (~p, ~p)~n", [self(), To, L, Avg, {get(gs2_pq)}]),
                     L;
                true ->
                     Limit0
             end,
    put({credit_limit, To}, Limit1).
    %% ok.

unblock() ->
    erase(credit_blocked),
    case get(credit_deferred) of
        undefined -> ok;
        Deferred  -> [To ! Msg || {To, Msg} <- Deferred],
                     erase(credit_deferred)
    end.
