%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_observer_cli_classic_queues).

-export([plugin_info/0]).
-export([attributes/1, sheet_header/0, sheet_body/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

plugin_info() ->
    #{
        module => rabbit_observer_cli_classic_queues,
        title => "Classic",
        shortcut => "CQ",
        sort_column => 4
    }.

attributes(State) ->
    Content1 = "Location of messages in classic queues.",
    Content2 = "Q, MQ and GQ are Erlang messages (total, mailbox and GS2 queue).",
    Content3 = "mem/disk are AMQP messages in memory or on disk.",
    Content4 = "pa/cf are messages pending acks or confirms.",
    Content5 = "qib/qibu/qsb are index/store buffer sizes, with qib = AMQP messages + qibu (acks).",
    {[
        [#{content => Content1, width => 136}],
        [#{content => Content2, width => 136}],
        [#{content => Content3, width => 136}],
        [#{content => Content4, width => 136}],
        [#{content => Content5, width => 136}]
    ], State}.

sheet_header() ->
    [
        #{title => "Pid", width => 12, shortcut => "P"},
        #{title => "Vhost", width => 10, shortcut => "V"},
        #{title => "Name", width => 26, shortcut => "N"},
        #{title => "", width => 6, shortcut => "Q"},
        #{title => "", width => 6, shortcut => "MQ"},
        #{title => "", width => 6, shortcut => "GQ"},
        #{title => "", width => 10, shortcut => "mem"},
        #{title => "", width => 10, shortcut => "disk"},
        #{title => "", width => 8, shortcut => "pa"},
        #{title => "", width => 8, shortcut => "cf"},
        #{title => "", width => 6, shortcut => "qib"},
        #{title => "", width => 6, shortcut => "qibu"},
        #{title => "", width => 6, shortcut => "qsb"}
    ].

sheet_body(State) ->
    Body = [begin
        #resource{name = Name, virtual_host = Vhost} = amqqueue:get_name(Q),
        case rabbit_amqqueue:pid_of(Q) of
            none ->
                ["dead", Vhost, unicode:characters_to_binary([Name, " (dead)"]),
                    0,0,0,0,0,0,0,0,0,0];
            Pid ->
                ProcInfo = erpc:call(node(Pid), erlang, process_info, [Pid, message_queue_len]),
                case ProcInfo of
                    undefined ->
                        [Pid, Vhost, unicode:characters_to_binary([Name, " (dead)"]),
                            0,0,0,0,0,0,0,0,0,0];
                    {_, MsgQ} ->
                        GS2Q = erpc:call(node(Pid), rabbit_core_metrics, get_gen_server2_stats, [Pid]),
                        Info = rabbit_amqqueue:info(Q),
                        BQInfo = proplists:get_value(backing_queue_status, Info),
                        [
                            Pid, Vhost, Name,
                            format_int(MsgQ + GS2Q), format_int(MsgQ), format_int(GS2Q),
                            proplists:get_value(q3, BQInfo),
                            element(3, proplists:get_value(delta, BQInfo)),
                            proplists:get_value(num_pending_acks, BQInfo),
                            proplists:get_value(num_unconfirmed, BQInfo),
                            proplists:get_value(qi_buffer_size, BQInfo, 0),
                            proplists:get_value(qi_buffer_num_up, BQInfo, 0),
                            proplists:get_value(qs_buffer_size, BQInfo)
                        ]
                end
        end
    end || Q <- list_classic_queues()],
    {Body, State}.

%% This function gets all classic queues regardless of durable/exclusive status.
list_classic_queues() ->
    rabbit_db_queue:get_all_by_type(rabbit_classic_queue).

format_int(N) when N >= 1_000_000_000 ->
    integer_to_list(N div 1_000_000_000) ++ "B";
format_int(N) when N >= 1_000_000 ->
    integer_to_list(N div 1_000_000) ++ "M";
%% We print up to 9999 messages and shorten 10K+.
format_int(N) when N >= 10_000 ->
    integer_to_list(N div 1_000) ++ "K";
format_int(N) ->
    N.
