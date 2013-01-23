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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_user).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, put_user/1]).

-import(rabbit_misc, [pget/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case user(ReqData) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, User} = user(ReqData),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:internal_user(User),
                           ReqData, Context).

accept_content(ReqData, Context) ->
    Username = rabbit_mgmt_util:id(user, ReqData),
    rabbit_mgmt_util:with_decode(
      [], ReqData, Context,
      fun(_, User) ->
              put_user([{name, Username} | User]),
              {true, ReqData, Context}
      end).

delete_resource(ReqData, Context) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    rabbit_auth_backend_internal:delete_user(User),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

user(ReqData) ->
    rabbit_auth_backend_internal:lookup_user(rabbit_mgmt_util:id(user, ReqData)).

put_user(User) ->
    CP = fun rabbit_auth_backend_internal:change_password/2,
    CPH = fun rabbit_auth_backend_internal:change_password_hash/2,
    case {proplists:is_defined(password, User),
          proplists:is_defined(password_hash, User)} of
        {true, _} -> put_user(User, pget(password, User), CP);
        {_, true} -> Hash = rabbit_mgmt_util:b64decode_or_throw(
                              pget(password_hash, User)),
                     put_user(User, Hash, CPH);
        _         -> put_user(User, <<>>, CPH)
    end.

put_user(User, PWArg, PWFun) ->
    Username = pget(name, User),
    Tags = case {pget(tags, User), pget(administrator, User)} of
               {undefined, undefined} ->
                   throw({error, tags_not_present});
               {undefined, AdminS} ->
                   case rabbit_mgmt_util:parse_bool(AdminS) of
                       true  -> [administrator];
                       false -> []
                   end;
               {TagsS, _} ->
                   [list_to_atom(string:strip(T)) ||
                       T <- string:tokens(binary_to_list(TagsS), ",")]
           end,
    case rabbit_auth_backend_internal:lookup_user(Username) of
        {error, not_found} ->
            rabbit_auth_backend_internal:add_user(
              Username, rabbit_guid:binary(rabbit_guid:gen_secure(), "tmp"));
        _ ->
            ok
    end,
    PWFun(Username, PWArg),
    ok = rabbit_auth_backend_internal:set_tags(Username, Tags).
