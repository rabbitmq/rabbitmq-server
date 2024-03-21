%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ListWebMqttConnectionsCommand').

-include_lib("rabbitmq_mqtt/include/rabbit_mqtt.hrl").

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

formatter() -> 'Elixir.RabbitMQ.CLI.Formatters.Table'.
scopes() -> [ctl, diagnostics].
switches() -> [{verbose, boolean}].
aliases() -> [{'V', verbose}].

description() -> <<"Lists all Web MQTT connections">>.

help_section() ->
    {plugin, web_mqtt}.

validate(Args, _) ->
    InfoItems = lists:map(fun atom_to_list/1, ?INFO_ITEMS),
    case 'Elixir.RabbitMQ.CLI.Ctl.InfoKeys':validate_info_keys(Args,
                                                               InfoItems) of
        {ok, _} -> ok;
        Error   -> Error
    end.

merge_defaults([], Opts) ->
    merge_defaults([<<"client_id">>, <<"conn_name">>], Opts);
merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{verbose => false}, Opts)}.

usage() ->
    <<"list_web_mqtt_connections [<column> ...]">>.

usage_additional() ->
    Prefix = <<" must be one of ">>,
    InfoItems = 'Elixir.Enum':join(lists:usort(?INFO_ITEMS), <<", ">>),
    [
      {<<"<column>">>, <<Prefix/binary, InfoItems/binary>>}
    ].

usage_doc_guides() ->
    [<<"https://rabbitmq.com/docs/web-mqtt">>].

run(Args, #{node := NodeName,
            timeout := Timeout,
            verbose := Verbose}) ->
    InfoKeys = case Verbose of
        true  -> ?INFO_ITEMS;
        false -> 'Elixir.RabbitMQ.CLI.Ctl.InfoKeys':prepare_info_keys(Args)
    end,

    Nodes = 'Elixir.RabbitMQ.CLI.Core.Helpers':nodes_in_cluster(NodeName),

    'Elixir.RabbitMQ.CLI.Ctl.RpcStream':receive_list_items(
        NodeName,
        rabbit_web_mqtt_app,
        emit_connection_info_all,
        [Nodes, InfoKeys],
        Timeout,
        InfoKeys,
        length(Nodes)).

banner(_, _) -> <<"Listing Web MQTT connections ...">>.

output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).
