%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.RestartFederationLinkCommand').

-include("rabbit_federation.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         flags/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         aliases/0,
         output/2,
         help_section/0,
         description/0
        ]).


%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------
usage() ->
     <<"restart_federation_link <link_id>">>.

usage_additional() ->
   [
      {<<"<link_id>">>, <<"ID of the link to restart">>}
   ].

usage_doc_guides() ->
    [?FEDERATION_GUIDE_URL].

help_section() ->
   {plugin, federation}.

description() ->
   <<"Restarts a running federation link">>.

flags() ->
    [].

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_, _ | _], _Opts) ->
    {validation_failure, too_many_args};
validate([_], _) ->
    ok.

merge_defaults(A, O) ->
    {A, O}.

banner([Link], #{node := Node}) ->
    erlang:iolist_to_binary([<<"Restarting federation link ">>, Link, << " on node ">>,
                             atom_to_binary(Node, utf8)]).

run([Id], #{node := Node}) ->
    case rabbit_misc:rpc_call(Node, rabbit_federation_status, lookup, [Id]) of
        {badrpc, _} = Error ->
            Error;
        not_found ->
            {error, <<"Link with provided ID was not found">>};
        Obj ->
            Upstream = proplists:get_value(upstream, Obj),
            Supervisor = proplists:get_value(supervisor, Obj),
            rabbit_misc:rpc_call(Node, rabbit_federation_link_sup, restart,
                                 [Supervisor, Upstream])
    end.

aliases() ->
    [].

output(Output, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Output).
