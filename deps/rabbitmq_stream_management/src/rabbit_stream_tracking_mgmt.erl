%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_tracking_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         resource_exists/2,
         to_json/2,
         content_types_provided/2,
         is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

dispatcher() ->
    [{"/stream/:vhost/:queue/tracking", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found ->
             false;
         none ->
             false; % none means `all`
         _ ->
             case rabbit_mgmt_util:id(queue, ReqData) of
                 none ->
                     false;
                 _ ->
                     case rabbit_mgmt_wm_queue:queue(ReqData) of
                         not_found ->
                             false;
                         _ ->
                             true
                     end
             end
     end,
     ReqData, Context}.

to_json(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            VHost = rabbit_mgmt_util:vhost(ReqData),
            Stream = rabbit_mgmt_util:id(queue, ReqData),
            case rabbit_stream_manager:lookup_leader(VHost, Stream) of
                {ok, Leader} ->
                    Type = tracking_type(rabbit_mgmt_util:get_value_param(<<"type">>, ReqData)),
                    TrackingInfo = maps:remove(timestamps, osiris:read_tracking(Leader)),
                    rabbit_mgmt_util:reply(transform_tracking(Type, TrackingInfo),
                                           ReqData,
                                           Context);
                {error, _} ->
                    rabbit_mgmt_util:service_unavailable(<<"The stream leader is not available">>,
                                                         ReqData, Context)
            end;
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>,
                                         ReqData, Context)
    end.

tracking_type(undefined) ->
    all;
tracking_type("offset") ->
    offset;
tracking_type("writer") ->
    writer;
tracking_type(_) ->
    all.

transform_tracking(offset, Tracking) ->
    maps:remove(sequences, Tracking);
transform_tracking(writer, Tracking) ->
    #{writers => convert_writer_tracking(maps:get(sequences, Tracking))};
transform_tracking(all, Tracking) ->
    #{offsets => maps:get(offsets, Tracking),
      writers => convert_writer_tracking(maps:get(sequences, Tracking))}.

convert_writer_tracking(Writers) ->
    maps:fold(fun(Ref, {_, Seq}, Acc) ->
                      Acc#{Ref => Seq}    
              end, #{}, Writers).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).
