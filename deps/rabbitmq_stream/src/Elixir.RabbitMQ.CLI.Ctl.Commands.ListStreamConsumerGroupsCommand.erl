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
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumerGroupsCommand').

-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref([{'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}]).

-export([formatter/0,
         scopes/0,
         aliases/0,
         usage/0,
         usage_doc_guides/0,
         banner/2,
         validate/2,
         merge_defaults/2,
         run/2,
         output/2,
         description/0,
         help_section/0]).

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.Table'.

scopes() ->
    [ctl, diagnostics, streams].

aliases() ->
    [{'V', verbose}].

description() ->
    <<"Lists groups of stream single active consumers "
      "for a vhost">>.

help_section() ->
    {plugin, stream}.

validate(_Args, _Opts) ->
    ok.

merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{vhost => <<"/">>}, Opts)}.

usage() ->
    <<"list_stream_consumer_groups [--vhost <vhost>]">>.

usage_doc_guides() ->
    [?STREAM_GUIDE_URL].

run(_Args,
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout}) ->
    rabbit_misc:rpc_call(NodeName,
                         rabbit_stream_coordinator,
                         consumer_groups,
                         [VHost],
                         Timeout).

banner(_, _) ->
    <<"Listing stream consumer groups ...">>.

output({ok, []}, _Opts) ->
    ok;
output([], _Opts) ->
    ok;
output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).
