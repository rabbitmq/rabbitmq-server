%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_wm_user).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, undefined}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_access_control:lookup_user(id(ReqData)) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, User} = rabbit_access_control:lookup_user(id(ReqData)),
    {rabbit_mgmt_format:encode([{user, User#user.username}]), ReqData, Context}.

accept_content(ReqData, Context) ->
    User = id(ReqData),
    case decode(["password"], ReqData) of
        [Pass] ->
            case rabbit_access_control:lookup_user(User) of
                {ok, _} ->
                    rabbit_access_control:change_password(User, Pass);
                {error, not_found} ->
                    rabbit_access_control:add_user(User, Pass)
            end,
            {true, ReqData, Context};
        {error, Reason} ->
            {{halt, 400}, ReqData, Context}
    end.

delete_resource(ReqData, Context) ->
    User = id(ReqData),
    rabbit_access_control:delete_user(User),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

id(ReqData) ->
    {ok, Id} = dict:find(user, wrq:path_info(ReqData)),
    list_to_binary(mochiweb_util:unquote(Id)).

decode(Keys, ReqData) ->
    Body = wrq:req_body(ReqData),
    {Res, Json} = try
                      {struct, J} = mochijson2:decode(Body),
                      {ok, J}
                  catch error:_ -> {error, not_json}
                  end,
    case Res of
        ok ->
            Results =
                [proplists:get_value(list_to_binary(K), Json) || K <- Keys],
            case lists:any(fun(E) -> E == undefined end, Results) of
                false -> Results;
                true  -> {error, key_missing}
            end;
        _  ->
            {Res, Json}
    end.
