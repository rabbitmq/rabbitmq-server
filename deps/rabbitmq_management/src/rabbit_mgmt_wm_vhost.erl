%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_vhost).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, id/1, put_vhost/3]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {rabbit_vhost:exists(id(ReqData)), ReqData, Context}.

to_json(ReqData, Context) ->
    try
        rabbit_mgmt_util:reply(
          hd(rabbit_mgmt_db:augment_vhosts(
               [rabbit_vhost:info(id(ReqData))], rabbit_mgmt_util:range(ReqData))),
          ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    Name = id(ReqData0),
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun(_, VHost, ReqData) ->
              Trace = rabbit_mgmt_util:parse_bool(maps:get(tracing, VHost, undefined)),
              case put_vhost(Name, Trace, Username) of
                  ok               ->
                      {true, ReqData, Context};
                  {error, timeout} = E ->
                      rabbit_mgmt_util:internal_server_error(
                        "Timed out while waiting for the vhost to initialise", E,
                        ReqData0, Context)
              end
      end).

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    VHost = id(ReqData),
    try
        rabbit_vhost:delete(VHost, Username)
    catch _:{error, {no_such_vhost, _}} ->
        ok
    end,
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

id(ReqData) ->
    rabbit_mgmt_util:id(vhost, ReqData).

put_vhost(Name, Trace, Username) ->
    Result = case rabbit_vhost:exists(Name) of
        true  -> ok;
        false -> rabbit_vhost:add(Name, Username),
                 %% wait for up to 45 seconds for the vhost to initialise
                 %% on all nodes
                 case rabbit_vhost:await_running_on_all_nodes(Name, 45000) of
                     ok               ->
                         maybe_grant_full_permissions(Name, Username),
                         case Trace of
                             true      -> rabbit_trace:start(Name);
                             false     -> rabbit_trace:stop(Name);
                             undefined -> ok
                         end;
                     {error, timeout} ->
                         {error, timeout}
                 end
    end,
    case Trace of
        true      -> rabbit_trace:start(Name);
        false     -> rabbit_trace:stop(Name);
        undefined -> ok
    end,
    Result.


%% when definitions are loaded on boot, Username here will be ?INTERNAL_USER,
%% which does not actually exist
maybe_grant_full_permissions(_Name, ?INTERNAL_USER) ->
    ok;
maybe_grant_full_permissions(Name, Username) ->
    U = rabbit_auth_backend_internal:lookup_user(Username),
    maybe_grant_full_permissions(U, Name, Username).

maybe_grant_full_permissions({ok, _}, Name, Username) ->
    rabbit_auth_backend_internal:set_permissions(
      Username, Name, <<".*">>, <<".*">>, <<".*">>, Username);
maybe_grant_full_permissions(_, _Name, _Username) ->
    ok.
