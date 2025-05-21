%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_observer_cli_quorum_queues).

-export([add_plugin/0, plugin_info/0]).
-export([attributes/1, sheet_header/0, sheet_body/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "Quorum queues observer_cli plugin"},
                    {mfa,         {?MODULE, add_plugin, []}},
                    {requires,    [rabbit_observer_cli]},
                    {enables,     routing_ready}]}).

add_plugin() ->
    rabbit_observer_cli:add_plugin(plugin_info()).

plugin_info() ->
    #{
        module => rabbit_observer_cli_quorum_queues,
        title => "Quorum",
        shortcut => "QQ",
        sort_column => 4
    }.

attributes(State) ->
    Content1 = "S - leader/follower, MsgQ - Erlang mailbox queue, CMD - commands, SIW - snapshots installed/written",
    Content2 = "SS - snapshots sent, SW - snapshots written, MS - messages sent",
    Content3 = "E - elections, WOp - write operations, WRe - write resends, GC - Ra forced GC",
    Content4 = "CT - current term, SnapIdx - snapshot index, LA - last applied, CI - current index, LW - last written log entry index, CL - commit latency",
    RaCounters = ra_counters:overview(),
    {
    [ra_log_wal_header()] ++ [ra_log_wal_sheet(RaCounters)] ++
    [ra_log_segment_writer_header()] ++ [ra_log_segment_writer_sheet(RaCounters)] ++
    [
        [#{content => "", width => 136}],
        [#{content => Content1, width => 136}],
        [#{content => Content2, width => 136}],
        [#{content => Content3, width => 136}],
        [#{content => Content4, width => 136}],
        [#{content => "", width => 136}]
    ]
     , State}.

ra_log_wal_header() ->
    [
        #{content => " ra_log_wal", width => 25, color => <<"\e[7m">>},
        #{content => " MsgQ ", width => 8, color => <<"\e[7m">>},
        #{content => " WAL files", width => 11, color => <<"\e[7m">>},
        #{content => " Bytes written", width => 17, color => <<"\e[7m">>},
        #{content => " Writes", width => 16, color => <<"\e[7m">>},
        #{content => " Batches", width => 15, color => <<"\e[7m">>},
        #{content => " Bytes/Batch", width => 15, color => <<"\e[7m">>},
        #{content => " Writes/Batch", width => 22, color => <<"\e[7m">>}
    ].

ra_log_wal_sheet(RaCounters) ->
    RaLogWalCounters = maps:get(ra_log_wal, RaCounters),
    RaLogWalInfo = erlang:process_info(whereis(ra_log_wal), [message_queue_len]),
    [
        #{content => "", width => 24},
        #{content => proplists:get_value(message_queue_len, RaLogWalInfo), width => 6},
        #{content => maps:get(wal_files, RaLogWalCounters), width => 9},
        #{content => {byte, maps:get(bytes_written, RaLogWalCounters)}, width => 15},
        #{content => maps:get(writes, RaLogWalCounters), width => 14},
        #{content => maps:get(batches, RaLogWalCounters), width => 13},
        #{content => {byte, case maps:get(batches, RaLogWalCounters) of
                                0 -> 0;
                                Batches -> maps:get(bytes_written, RaLogWalCounters) / Batches
                            end}, width => 13},
        #{content => case maps:get(batches, RaLogWalCounters) of
                         0 -> 0;
                         Batches -> maps:get(writes, RaLogWalCounters) / Batches
                     end, width => 21}
    ].

ra_log_segment_writer_header() ->
    [
        #{content => " ra_log_segment_writer", width => 25, color => <<"\e[7m">>},
        #{content => " Bytes written", width => 17, color => <<"\e[7m">>},
        #{content => " Mem Tables", width => 16, color => <<"\e[7m">>},
        #{content => " Entries", width => 15, color => <<"\e[7m">>},
        #{content => " Segments", width => 59, color => <<"\e[7m">>}
    ].

ra_log_segment_writer_sheet(RaCounters) ->
    RaLogWalInfo = maps:get(ra_log_segment_writer, RaCounters),
    [
        #{content => "", width => 24},
        #{content => {byte, maps:get(bytes_written, RaLogWalInfo)}, width => 15},
        #{content => maps:get(mem_tables, RaLogWalInfo), width => 14},
        #{content => maps:get(entries, RaLogWalInfo), width => 13},
        #{content => maps:get(segments, RaLogWalInfo), width => 58}
    ].
sheet_header() ->
    [
        #{title => "Pid", width => 10, shortcut => ""},
        #{title => "Name", width => 8, shortcut => ""},
        #{title => "S", width => 3, shortcut => ""},
        #{title => "Memory", width => 10, shortcut => "M"},
        #{title => "", width => 6, shortcut => "MsgQ"},
        #{title => "", width => 7, shortcut => "CMD"},
        #{title => "", width => 6, shortcut => "SI/W"},
        #{title => "", width => 5, shortcut => "SS"},
        #{title => "", width => 7, shortcut => "MS"},
        #{title => "", width => 5, shortcut => "E"},
        #{title => "", width => 7, shortcut => "WOps"},
        #{title => "", width => 5, shortcut => "WRe"},
        #{title => "", width => 4, shortcut => "CT"},
        #{title => "SnapIdx", width => 8, shortcut => ""},
        #{title => "", width => 7, shortcut => "LA"},
        #{title => "", width => 6, shortcut => "CI"},
        #{title => "", width => 6, shortcut => "LW"},
        #{title => "", width => 5, shortcut => "CL"}
    ].

sheet_body(PrevState) ->
    {_, RaStates} = rabbit_quorum_queue:all_replica_states(),
    Body = [begin
                #resource{name = Name, virtual_host = Vhost} = amqqueue:get_name(Q),
                case rabbit_amqqueue:pid_of(Q) of
                    none ->
                        empty_row(Name);
                    {QName, _QNode} = ServerId ->
                        case whereis(QName) of
                            undefined ->
                                empty_row(Name);
                            Pid ->
                                ProcInfo = erlang:process_info(Pid, [message_queue_len, memory]),
                                case ProcInfo of
                                    undefined ->
                                        empty_row(Name);
                                    _ ->
                                        QQCounters = maps:get({QName, node()}, ra_counters:overview()),
                                        {ok, InternalName} = rabbit_queue_type_util:qname_to_internal_name(#resource{virtual_host = Vhost, name= Name}),
                                        #{snapshot_index := SnapIdx,
                                            last_written_index := LW,
                                            term := CT,
                                            commit_latency := CL,
                                            commit_index := CI,
                                            last_applied := LA} = ra:key_metrics(ServerId),
                                        [
                                         Pid,
                                         QName,
                                         case maps:get(InternalName, RaStates, undefined) of
                                             leader -> "L";
                                             follower -> "F";
                                             promotable -> "f";  %% temporary non-voter
                                             non_voter -> "-";  %% permanent non-voter
                                             _ -> "?"
                                         end,
                                         format_int(proplists:get_value(memory, ProcInfo)),
                                         format_int(proplists:get_value(message_queue_len, ProcInfo)),
                                         format_int(maps:get(commands, QQCounters)),
                                         case maps:get(InternalName, RaStates, undefined) of
                                             leader -> format_int(maps:get(snapshots_written, QQCounters));
                                             follower -> format_int(maps:get(snapshot_installed, QQCounters));
                                             _ -> "?"
                                         end,
                                         format_int(maps:get(snapshots_sent, QQCounters)),
                                         format_int(maps:get(msgs_sent, QQCounters)),
                                         format_int(maps:get(elections, QQCounters)),
                                         format_int(maps:get(write_ops, QQCounters)),
                                         format_int(maps:get(write_resends, QQCounters)),
                                         CT, SnapIdx, LA, CI, LW, CL
                                        ]
                                end
                        end
                end
            end || Q <- list_quorum_queues()],
    {Body, PrevState}.

list_quorum_queues() ->
    rabbit_db_queue:get_all_by_type(rabbit_quorum_queue).

format_int(N) when N >= 1_000_000_000 ->
    integer_to_list(N div 1_000_000_000) ++ "B";
format_int(N) when N >= 1_000_000 ->
    integer_to_list(N div 1_000_000) ++ "M";
%% We print up to 9999 messages and shorten 10K+.
format_int(N) when N >= 10_000 ->
    integer_to_list(N div 1_000) ++ "K";
format_int(N) ->
    N.

empty_row(Name) ->
    ["-", unicode:characters_to_binary([Name, " (dead)"])
        | [0 || _ <- lists:seq(1, length(sheet_header()) - 2)] ].
