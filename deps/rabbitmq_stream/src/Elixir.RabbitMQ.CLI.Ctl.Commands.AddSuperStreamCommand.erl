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
    [ctl, streams].

description() ->
    <<"Add a super stream (experimental feature)">>.

switches() ->
    [{partitions, integer}, {routing_keys, string}].

help_section() ->
    {plugin, stream}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_Name], #{partitions := _, routing_keys := _}) ->
    {validation_failure,
     "Specify --partitions or routing-keys, not both."};
validate([_Name], #{partitions := Partitions}) when Partitions < 1 ->
    {validation_failure, "The partition number must be greater than 0"};
validate([_Name], _Opts) ->
    ok;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

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
      "exclusive with --partitions."]].

usage_doc_guides() ->
    [?STREAM_GUIDE_URL].

run([SuperStream],
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout,
      partitions := Partitions}) ->
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
                        RoutingKeys).

create_super_stream(NodeName,
                    Timeout,
                    VHost,
                    SuperStream,
                    Streams,
                    RoutingKeys) ->
    case rabbit_misc:rpc_call(NodeName,
                              rabbit_stream_manager,
                              create_super_stream,
                              [VHost,
                               SuperStream,
                               Streams,
                               [],
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
    <<"Adding a super stream ...">>.

output({error, Msg}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(), Msg};
output({ok, Msg}, _Opts) ->
    {ok, Msg}.

cli_acting_user() ->
    'Elixir.RabbitMQ.CLI.Core.Helpers':cli_acting_user().
