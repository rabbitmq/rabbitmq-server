%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_rabbit_queue_consumers_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-import(rabbit_queue_consumers, [subtract_acks/4]).

all() ->
    [
        {group, default}
    ].

groups() ->
    [
        {default, [], [
            ack_fifo, ack_multiple, ack_middle, ack_lifo
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

ack_fifo(_Config) ->
    compare({ctag_count(1), ctag_queue(1, 2)}, subtract_acks([0], [], maps:new(), ctag_queue(2))),
    ok.

ack_multiple(_Config) ->
    compare({ctag_count(4), ctag_queue(4, 9)}, subtract_acks([0, 1, 2, 3], [], maps:new(), ctag_queue(9))),
    ok.

ack_middle(_Config) ->
    compare({ctag_count(1), ctag_queue(0, 9, [4])}, subtract_acks([4], [], maps:new(), ctag_queue(9))),
    ok.

ack_lifo(_Config) ->
    compare({ctag_count(1), ctag_queue(0, 8)}, subtract_acks([9], [], maps:new(), ctag_queue(9))),
    ok.

compare({ExpectedCTagsCount, ExpectedAckQ}, Result) ->
    {CTagsCount, AckQ} = Result,
    ExpectedCTagsCount = CTagsCount,
    AckList = queue:to_list(AckQ),
    AckList = queue:to_list(ExpectedAckQ),
    ok.

ctag_queue(To) ->
    ctag_queue(0, To).

ctag_queue(From, To) ->
    ctag_queue(From, To, []).

ctag_queue(From, To, Excluded) ->
    queue:from_list([{Value, <<"ctag">>} || Value <- lists:filter(fun (X) -> not lists:member(X, Excluded) end, lists:seq(From, To))]).

ctag_count(Count) ->
    #{ <<"ctag">> => Count}.


