%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.AddSuperStreamCommand').

-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref([{'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1},
              {'Elixir.RabbitMQ.CLI.Core.Helpers', cli_acting_user, 0},
              {'Elixir.RabbitMQ.CLI.Core.ExitCodes', exit_software, 0}]).

-export([scopes/0,
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         switches/0,
         banner/2,
         validate/2,
         merge_defaults/2,
         run/2,
         output/2,
         description/0,
         help_section/0]).

scopes() ->
    [streams].

description() ->
    <<"Add a super stream (experimental feature)">>.

switches() ->
    [{partitions, integer},
     {routing_keys, string},
     {max_length_bytes, string},
     {max_age, string},
     {stream_max_segment_size_bytes, string},
     {leader_locator, string},
     {initial_cluster_size, integer}].

help_section() ->
    {plugin, stream}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_Name], #{partitions := _, routing_keys := _}) ->
    {validation_failure,
     "Specify --partitions or routing-keys, not both."};
validate([_Name], #{partitions := Partitions}) when Partitions < 1 ->
    {validation_failure, "The partition number must be greater than 0"};
validate([_Name], Opts) ->
    validate_stream_arguments(Opts);
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

validate_stream_arguments(#{max_length_bytes := Value} = Opts) ->
    case parse_information_unit(Value) of
        error ->
            {validation_failure,
             "Invalid value for --max-length-bytes, valid example "
             "values: 100gb, 50mb"};
        _ ->
            validate_stream_arguments(maps:remove(max_length_bytes, Opts))
    end;
validate_stream_arguments(#{max_age := Value} = Opts) ->
    case rabbit_date_time:parse_duration(Value) of
        {ok, _} ->
            validate_stream_arguments(maps:remove(max_age, Opts));
        error ->
            {validation_failure,
             "Invalid value for --max-age, the value must a "
             "ISO 8601 duration, e.g. e.g. PT10M30S for 10 "
             "minutes 30 seconds, P5DT8H for 5 days 8 hours."}
    end;
validate_stream_arguments(#{stream_max_segment_size_bytes := Value} =
                              Opts) ->
    case parse_information_unit(Value) of
        error ->
            {validation_failure,
             "Invalid value for --stream-max-segment-size-bytes, "
             "valid example values: 100gb, 50mb"};
        _ ->
            validate_stream_arguments(maps:remove(stream_max_segment_size_bytes,
                                                  Opts))
    end;
validate_stream_arguments(#{leader_locator := <<"client-local">>} =
                              Opts) ->
    validate_stream_arguments(maps:remove(leader_locator, Opts));
validate_stream_arguments(#{leader_locator := <<"random">>} = Opts) ->
    validate_stream_arguments(maps:remove(leader_locator, Opts));
validate_stream_arguments(#{leader_locator := <<"least-leaders">>} =
                              Opts) ->
    validate_stream_arguments(maps:remove(leader_locator, Opts));
validate_stream_arguments(#{leader_locator := _}) ->
    {validation_failure,
     "Invalid value for --leader-locator, valid values "
     "are client-local, random, least-leaders."};
validate_stream_arguments(#{initial_cluster_size := Value} = Opts) ->
    try
        case rabbit_data_coercion:to_integer(Value) of
            S when S > 0 ->
                validate_stream_arguments(maps:remove(initial_cluster_size,
                                                      Opts));
            _ ->
                {validation_failure,
                 "Invalid value for --initial-cluster-size, the "
                 "value must be positive."}
        end
    catch
        error:_ ->
            {validation_failure,
             "Invalid value for --initial-cluster-size, the "
             "value must be a positive integer."}
    end;
validate_stream_arguments(_) ->
    ok.

merge_defaults(_Args, #{routing_keys := _V} = Opts) ->
    {_Args, maps:merge(#{vhost => <<"/">>}, Opts)};
merge_defaults(_Args, Opts) ->
    {_Args, maps:merge(#{partitions => 3, vhost => <<"/">>}, Opts)}.

usage() ->
    <<"add_super_stream <name> [--vhost <vhost>] [--partition"
      "s <partitions>] [--routing-keys <routing-keys>]">>.

usage_additional() ->
    [["<name>", "The name of the super stream."],
     ["--vhost <vhost>", "The virtual host the super stream is added to."],
     ["--partitions <partitions>",
      "The number of partitions, default is 3. Mutually "
      "exclusive with --routing-keys."],
     ["--routing-keys <routing-keys>",
      "Comma-separated list of routing keys. Mutually "
      "exclusive with --partitions."],
     ["--max-length-bytes <max-length-bytes>",
      "The maximum size of partition streams, example "
      "values: 20gb, 500mb."],
     ["--max-age <max-age>",
      "The maximum age of partition stream segments, "
      "using the ISO 8601 duration format, e.g. PT10M30S "
      "for 10 minutes 30 seconds, P5DT8H for 5 days "
      "8 hours."],
     ["--stream-max-segment-size-bytes <stream-max-segment-si"
      "ze-bytes>",
      "The maximum size of partition stream segments, "
      "example values: 500mb, 1gb."],
     ["--leader-locator <leader-locator>",
      "Leader locator strategy for partition streams, "
      "possible values are client-local, least-leaders, "
      "random."],
     ["--initial-cluster-size <initial-cluster-size>",
      "The initial cluster size of partition streams."]].

usage_doc_guides() ->
    [?STREAM_GUIDE_URL].

run([SuperStream],
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout,
      partitions := Partitions} =
        Opts) ->
    Streams =
        [list_to_binary(binary_to_list(SuperStream)
                        ++ "-"
                        ++ integer_to_list(K))
         || K <- lists:seq(0, Partitions - 1)],
    RoutingKeys =
        [integer_to_binary(K) || K <- lists:seq(0, Partitions - 1)],
    create_super_stream(NodeName,
                        Timeout,
                        VHost,
                        SuperStream,
                        Streams,
                        stream_arguments(Opts),
                        RoutingKeys);
run([SuperStream],
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout,
      routing_keys := RoutingKeysStr} =
        Opts) ->
    RoutingKeys =
        [rabbit_data_coercion:to_binary(
             string:strip(K))
         || K
                <- string:tokens(
                       rabbit_data_coercion:to_list(RoutingKeysStr), ",")],
    Streams =
        [list_to_binary(binary_to_list(SuperStream)
                        ++ "-"
                        ++ binary_to_list(K))
         || K <- RoutingKeys],
    create_super_stream(NodeName,
                        Timeout,
                        VHost,
                        SuperStream,
                        Streams,
                        stream_arguments(Opts),
                        RoutingKeys).

stream_arguments(Opts) ->
    stream_arguments(#{}, Opts).

stream_arguments(Acc, Arguments) when map_size(Arguments) =:= 0 ->
    Acc;
stream_arguments(Acc, #{max_length_bytes := Value} = Arguments) ->
    stream_arguments(maps:put(<<"max-length-bytes">>,
                              parse_information_unit(Value), Acc),
                     maps:remove(max_length_bytes, Arguments));
stream_arguments(Acc, #{max_age := Value} = Arguments) ->
    {ok, Duration} = rabbit_date_time:parse_duration(Value),
    DurationInSeconds = duration_to_seconds(Duration),
    stream_arguments(maps:put(<<"max-age">>,
                              list_to_binary(integer_to_list(DurationInSeconds)
                                             ++ "s"),
                              Acc),
                     maps:remove(max_age, Arguments));
stream_arguments(Acc,
                 #{stream_max_segment_size_bytes := Value} = Arguments) ->
    stream_arguments(maps:put(<<"stream-max-segment-size-bytes">>,
                              parse_information_unit(Value), Acc),
                     maps:remove(stream_max_segment_size_bytes, Arguments));
stream_arguments(Acc, #{initial_cluster_size := Value} = Arguments) ->
    stream_arguments(maps:put(<<"initial-cluster-size">>,
                              rabbit_data_coercion:to_binary(Value), Acc),
                     maps:remove(initial_cluster_size, Arguments));
stream_arguments(Acc, #{leader_locator := Value} = Arguments) ->
    stream_arguments(maps:put(<<"queue-leader-locator">>, Value, Acc),
                     maps:remove(leader_locator, Arguments));
stream_arguments(ArgumentsAcc, _Arguments) ->
    ArgumentsAcc.

duration_to_seconds([{sign, _},
                     {years, Y},
                     {months, M},
                     {days, D},
                     {hours, H},
                     {minutes, Mn},
                     {seconds, S}]) ->
    Y * 365 * 86400 + M * 30 * 86400 + D * 86400 + H * 3600 + Mn * 60 + S.

create_super_stream(NodeName,
                    Timeout,
                    VHost,
                    SuperStream,
                    Streams,
                    Arguments,
                    RoutingKeys) ->
    case rabbit_misc:rpc_call(NodeName,
                              rabbit_stream_manager,
                              create_super_stream,
                              [VHost,
                               SuperStream,
                               Streams,
                               Arguments,
                               RoutingKeys,
                               cli_acting_user()],
                              Timeout)
    of
        ok ->
            {ok,
             rabbit_misc:format("Super stream ~s has been created",
                                [SuperStream])};
        Error ->
            Error
    end.

banner(_, _) ->
    <<"Adding a super stream (experimental feature)...">>.

output({error, Msg}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(), Msg};
output({ok, Msg}, _Opts) ->
    {ok, Msg}.

cli_acting_user() ->
    'Elixir.RabbitMQ.CLI.Core.Helpers':cli_acting_user().

parse_information_unit(Value) ->
    case rabbit_resource_monitor_misc:parse_information_unit(Value) of
        {ok, R} ->
            integer_to_binary(R);
        {error, _} ->
            error
    end.
