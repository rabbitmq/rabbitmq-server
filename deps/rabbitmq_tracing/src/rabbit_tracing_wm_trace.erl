%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_tracing_wm_trace).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-define(ERR, <<"Something went wrong trying to start the trace - check the "
               "logs.">>).

-import(rabbit_misc, [pget/2, pget/3]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------
init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.


content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{<<"application/json">>, accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case trace(ReqData, Context) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(trace(ReqData, Context), ReqData, Context).

accept_content(ReqData0, Ctx) ->
    case rabbit_mgmt_util:vhost(ReqData0) of
        not_found ->
            not_found;
        VHost ->
            Name = rabbit_mgmt_util:id(name, ReqData0),
            rabbit_mgmt_util:with_decode(
              [format, pattern], ReqData0, Ctx,
              fun([_, _], Trace, ReqData) ->
                      Fs = [fun val_payload_bytes/5, fun val_format/5,
                            fun val_create/5],
                      case lists:foldl(fun (F,  ok)  -> F(ReqData, Ctx, VHost,
                                                          Name, Trace);
                                           (_F, Err) -> Err
                                       end, ok, Fs) of
                          ok  -> {true, ReqData, Ctx};
                          Err -> rabbit_mgmt_util:bad_request(Err,
                                                              ReqData,
                                                              Ctx)
                      end
              end)
    end.

delete_resource(ReqData, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData),
    rabbit_tracing_util:apply_on_node(ReqData, Context, rabbit_tracing_traces, stop,
                                      [VHost, rabbit_mgmt_util:id(name, ReqData)]),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

trace(ReqData, Context) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     ->
            Name = rabbit_mgmt_util:id(name, ReqData),
            rabbit_tracing_util:apply_on_node(ReqData, Context, rabbit_tracing_traces,
                                              lookup, [VHost, Name])
    end.

val_payload_bytes(_ReqData, _Context, _VHost, _Name, Trace) ->
    case is_integer(maps:get(max_payload_bytes, Trace, 0)) of
        false -> <<"max_payload_bytes not integer">>;
        true  -> ok
    end.

val_format(_ReqData, _Context, _VHost, _Name, Trace) ->
    case lists:member(maps:get(format, Trace), [<<"json">>, <<"text">>]) of
        false -> <<"format not json or text">>;
        true  -> ok
    end.

val_create(ReqData, Context, VHost, Name, Trace) ->
    case rabbit_tracing_util:apply_on_node(
           ReqData, Context, rabbit_tracing_traces, create,
           [VHost, Name, maps:to_list(Trace)]) of
        {ok, _} -> ok;
        _       -> ?ERR
    end.
