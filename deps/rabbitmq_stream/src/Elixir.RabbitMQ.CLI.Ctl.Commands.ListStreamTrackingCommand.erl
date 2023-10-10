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
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamTrackingCommand').

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
         help_section/0,
         tracking_info/3]).

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.PrettyTable'.

scopes() ->
    [streams].

switches() ->
    [{stream, string}, {all, boolean}, {offset, boolean}, {writer, boolean}].

aliases() ->
    [].

description() ->
    <<"Lists tracking information for a stream">>.

help_section() ->
    {plugin, stream}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_Stream], Opts) ->
    case maps:with([all, writer, offset], Opts) of
        M when map_size(M) > 1 ->
            {validation_failure,
             "Specify only one of --all, --offset, --writer."};
        _ ->
            ok
    end;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

merge_defaults(Args, Opts) ->
    case maps:with([all, writer, offset], Opts) of
        M when map_size(M) =:= 0 ->
            {Args, maps:merge(#{all => true, vhost => <<"/">>}, Opts)};
        _ ->
            {Args, maps:merge(#{vhost => <<"/">>}, Opts)}
    end.

usage() ->
    <<"list_stream_tracking <stream> [--all | --offset | --writer] "
      "[--vhost <vhost>]">>.

usage_additional() ->
    [[<<"<name>">>,
      <<"The name of the stream.">>],
     [<<"--all">>,
      <<"List offset and writer tracking information.">>],
     [<<"--offset">>,
      <<"List only offset tracking information.">>],
     [<<"--writer">>,
      <<"List only writer deduplication tracking information.">>],
     [<<"--vhost <vhost>">>,
      <<"The virtual host of the stream.">>]].
 
usage_doc_guides() ->
    [?STREAM_GUIDE_URL].

run([Stream],
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout} = Opts) ->

    TrackingType = case Opts of
                       #{all := true} ->
                           all;
                       #{offset := true} ->
                           offset;
                       #{writer := true} ->
                           writer
                   end,
    case rabbit_misc:rpc_call(NodeName,
                         ?MODULE,
                         tracking_info,
                         [VHost, Stream, TrackingType],
                         Timeout) of
        {error, not_found} ->
            {error, "The stream does not exist."};
        {error, not_available} ->
            {error, "The stream is not available."};
        {error, _} = E ->
            E;
        R ->
            R
    end.
    
banner([Stream], _) ->
    <<"Listing tracking information for stream ", Stream/binary, <<" ...">>/binary>>.

output({ok, []}, _Opts) ->
    ok;
output([], _Opts) ->
    ok;
output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).

tracking_info(VHost, Stream, TrackingType) ->
    case rabbit_stream_manager:lookup_leader(VHost, Stream) of
        {ok, Leader} ->
            TrackingInfo = osiris:read_tracking(Leader),
            FieldsLabels = case TrackingType of
                               all ->
                                   [{offsets, offset}, {sequences, writer}];
                               offset ->
                                   [{offsets, offset}];
                               writer ->
                                   [{sequences, writer}]
                           end,
            lists:foldl(fun({F, L}, Acc) ->
                                Tracking = maps:get(F, TrackingInfo, #{}),
                                maps:fold(fun(Reference, {_, Sequence}, AccType) ->
                                                  [[{type, L},
                                                    {name, Reference},
                                                    {tracking_value, Sequence}
                                                   ] | AccType];
                                             (Reference, Offset, AccType) ->
                                                  [[{type, L},
                                                    {name, Reference},
                                                    {tracking_value, Offset}
                                                   ] | AccType]
                                          end, Acc, Tracking)
                        end, [], FieldsLabels);
        {error, _} = E ->
            E
    end.
