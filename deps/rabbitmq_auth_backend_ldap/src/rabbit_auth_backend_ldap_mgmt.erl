%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_ldap_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).

-export([init/2,
         content_types_accepted/2,
         allowed_methods/2,
         resource_exists/2,
         is_authorized/2,
         accept_content/2]).


-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_web_dispatch/include/rabbitmq_web_dispatch_records.hrl").

dispatcher() -> [{"/ldap/validate/bind/:name", ?MODULE, []}].

web_ui() -> [].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"PUT">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

accept_content(ReqData0, Context) ->
    F = fun (_Values, BodyMap, ReqData1) ->
        _Name = name(ReqData1),
        Port = rabbit_mgmt_util:parse_int(maps:get(port, BodyMap, 389)),
        _UseSsl = rabbit_mgmt_util:parse_bool(maps:get(use_ssl, BodyMap, false)),
        _UseStartTls = rabbit_mgmt_util:parse_bool(maps:get(use_starttls, BodyMap, false)),
        Servers = maps:get(servers, BodyMap, []),
        _Password = maps:get(password, BodyMap, <<"">>),
        Options = [
            {port, Port},
            {timeout, 5000},
            {ssl, false}
        ],
        ?LOG_DEBUG("eldap:open Servers: ~tp Options: ~tp", [Servers, Options]),
        case eldap:open(Servers, Options) of
            {ok, H} ->
                eldap:close(H),
                {true, ReqData1, Context};
            {error, E} ->
                Reason = unicode_format(E),
                rabbit_mgmt_util:bad_request(Reason, ReqData1, Context)
        end
    end,
    rabbit_mgmt_util:with_decode([], ReqData0, Context, F).

%%--------------------------------------------------------------------

name(ReqData) ->
    case rabbit_mgmt_util:id(name, ReqData) of
        [Value] -> Value;
        Value   -> Value
    end.

unicode_format(Arg) ->
    rabbit_data_coercion:to_utf8_binary(io_lib:format("~tp", [Arg])).
