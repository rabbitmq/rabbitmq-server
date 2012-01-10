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

-export([maybe_issue/1, bump/1, blocked/0, consume/1]).

maybe_issue(To) ->
    Credit =
        case get({credit_to, To}) of
            undefined ->
                ?MAX_CREDIT;
            ?MORE_CREDIT_AT + 1 ->
                To ! {bump_credit, {self(), ?MAX_CREDIT - ?MORE_CREDIT_AT}},
                ?MAX_CREDIT;
            C ->
                C - 1
        end,
    put({credit_to, To}, Credit).

bump({From, MoreCredit}) ->
    Credit = case get({credit_from, From}) of
                 undefined -> MoreCredit;
                 C         -> C + MoreCredit
             end,
    put({credit_from, From}, Credit),
    case Credit > 0 of
        true  -> erase(credit_blocked),
                 false;
        false -> true
    end.

%% TODO we assume only one From can block at once. Is this true?
blocked() ->
    get(credit_blocked) =:= true.

consume(From) ->
    Credit = case get({credit_from, From}) of
                 undefined -> ?MAX_CREDIT;
                 C         -> C
             end - 1,
    case Credit of
        0 -> put(credit_blocked, true);
        _ -> ok
    end,
    put({credit_from, From}, Credit).
