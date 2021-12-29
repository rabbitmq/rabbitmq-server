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

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteSuperStreamCommand').

-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref([{'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1},
              {'Elixir.RabbitMQ.CLI.Core.Helpers', cli_acting_user, 0},
              {'Elixir.RabbitMQ.CLI.Core.ExitCodes', exit_software, 0}]).

-export([scopes/0,
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

scopes() ->
    [streams].

description() ->
    <<"Delete a super stream (experimental feature)">>.

help_section() ->
    {plugin, stream}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_Name], _Opts) ->
    ok;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

merge_defaults(_Args, Opts) ->
    {_Args, maps:merge(#{vhost => <<"/">>}, Opts)}.

usage() ->
    <<"delete_super_stream <name> [--vhost <vhost>]">>.

usage_additional() ->
    [["<name>", "The name of the super stream to delete."],
     ["--vhost <vhost>", "The virtual host of the super stream."]].

usage_doc_guides() ->
    [?STREAM_GUIDE_URL].

run([SuperStream],
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout}) ->
    delete_super_stream(NodeName, Timeout, VHost, SuperStream).

delete_super_stream(NodeName, Timeout, VHost, SuperStream) ->
    case rabbit_misc:rpc_call(NodeName,
                              rabbit_stream_manager,
                              delete_super_stream,
                              [VHost, SuperStream, cli_acting_user()],
                              Timeout)
    of
        ok ->
            {ok,
             rabbit_misc:format("Super stream ~s has been deleted",
                                [SuperStream])};
        Error ->
            Error
    end.

banner(_, _) ->
    <<"Deleting a super stream (experimental feature)...">>.

output({error, Msg}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(), Msg};
output({ok, Msg}, _Opts) ->
    {ok, Msg}.

cli_acting_user() ->
    'Elixir.RabbitMQ.CLI.Core.Helpers':cli_acting_user().
