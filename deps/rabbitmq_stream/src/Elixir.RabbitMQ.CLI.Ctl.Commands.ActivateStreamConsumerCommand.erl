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
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ActivateStreamConsumerCommand').

-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([formatter/0,
         scopes/0,
         switches/0,
         aliases/0,
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         banner/2,
         validate/2,
         merge_defaults/2,
         run/2,
         output/2,
         description/0,
         help_section/0]).

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.String'.

scopes() ->
    [ctl, streams].

switches() ->
    [{stream, string}, {reference, string}].

aliases() ->
    [].

description() ->
    <<"Trigger a rebalancing to activate a consumer in "
      "a single active consumer group">>.

help_section() ->
    {plugin, stream}.

validate([], #{stream := _, reference := _}) ->
    ok;
validate(Args, _) when is_list(Args) andalso length(Args) > 0 ->
    {validation_failure, too_many_args};
validate(_, _) ->
    {validation_failure, not_enough_args}.

merge_defaults(_Args, Opts) ->
    {[], maps:merge(#{vhost => <<"/">>}, Opts)}.

usage() ->
    <<"activate_stream_consumer --stream <stream> "
      "--reference <reference> [--vhost <vhost>]">>.

usage_additional() ->
    <<"debugging command, use only when a group does not have "
      "an active consumer">>.

usage_doc_guides() ->
    [?STREAMS_GUIDE_URL].

run(_,
    #{node := NodeName,
      vhost := VHost,
      stream := Stream,
      reference := Reference,
      timeout := Timeout}) ->
    rabbit_misc:rpc_call(NodeName,
                         rabbit_stream_sac_coordinator,
                         activate_consumer,
                         [VHost, Stream, Reference],
                         Timeout).

banner(_, _) ->
    <<"Activating a consumer in the group ...">>.

output(ok, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output({ok,
                                                <<"OK">>});
output({error, not_found}, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output({error_string,
                                                <<"The group does not exist">>});
output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).
