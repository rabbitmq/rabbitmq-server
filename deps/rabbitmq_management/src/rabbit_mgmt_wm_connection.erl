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
-module(rabbit_mgmt_wm_connection).

-export([init/1, resource_exists/2, to_json/2, content_types_provided/2,
         is_authorized/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, undefined}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

id(ReqData) ->
    case dict:find(connection, wrq:path_info(ReqData)) of
        error    -> error;
        {ok, Id} -> list_to_binary(mochiweb_util:unquote(Id))
    end.

resource_exists(ReqData, Context) ->
    case id(ReqData) of
        error ->
            {true, ReqData, Context};
        Id ->
            case rabbit_mgmt_db:get_connection(Id) of
                error -> {false, ReqData, Context};
                _Conn -> {true, ReqData, Context}
            end
    end.

to_json(ReqData, Context) ->
    Res = case id(ReqData) of
              error ->
                  Conns = rabbit_mgmt_db:get_connections(),
                  {connections, [{struct, C} || C <- Conns]};
              Id ->
                  Conn = rabbit_mgmt_db:get_connection(Id),
                  {connection, {struct, Conn}}
          end,
    {rabbit_mgmt_format:encode([Res]), ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).
