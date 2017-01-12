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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_user).

-export([init/3, rest_init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, user/1, put_user/2, put_user/3]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case user(ReqData) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, User} = user(ReqData),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:internal_user(User),
                           ReqData, Context).

accept_content(ReqData, Context = #context{user = #user{username = ActingUser}}) ->
    Username = rabbit_mgmt_util:id(user, ReqData),
    rabbit_mgmt_util:with_decode(
      [], ReqData, Context,
      fun(_, User) ->
              put_user(User#{name => Username}, ActingUser),
              {true, ReqData, Context}
      end).

delete_resource(ReqData, Context = #context{user = #user{username = ActingUser}}) ->
    User = rabbit_mgmt_util:id(user, ReqData),
    rabbit_auth_backend_internal:delete_user(User, ActingUser),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

user(ReqData) ->
    rabbit_auth_backend_internal:lookup_user(rabbit_mgmt_util:id(user, ReqData)).

put_user(User, ActingUser) -> put_user(User, undefined, ActingUser).

put_user(User, Version, ActingUser) ->
    PasswordUpdateFun = 
        fun(Username) ->
                case {maps:is_key(password, User),
                      maps:is_key(password_hash, User)} of
                    {true, _} ->
                        rabbit_auth_backend_internal:change_password(
                          Username, maps:get(password, User, undefined),
                          ActingUser);
                    {_, true} ->
                        HashingAlgorithm = hashing_algorithm(User, Version),

                        Hash = rabbit_mgmt_util:b64decode_or_throw(
                                 maps:get(password_hash, User, undefined)),
                        rabbit_auth_backend_internal:change_password_hash(
                          Username, Hash, HashingAlgorithm);
                    _         ->
                        rabbit_auth_backend_internal:clear_password(Username, ActingUser)
                end
        end,
    put_user0(User, PasswordUpdateFun, ActingUser).

put_user0(User, PasswordUpdateFun, ActingUser) ->
    Username = maps:get(name, User, undefined),
    Tags = case {maps:get(tags, User, undefined), maps:get(administrator, User, undefined)} of
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
              Username, rabbit_guid:binary(rabbit_guid:gen_secure(), "tmp"), ActingUser);
        _ ->
            ok
    end,
    PasswordUpdateFun(Username),
    ok = rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser).

hashing_algorithm(User, Version) ->
    case maps:get(hashing_algorithm, User, undefined) of
        undefined ->
            case Version of
                %% 3.6.1 and later versions are supposed to have
                %% the algorithm exported and thus not need a default
                <<"3.6.0">>          -> rabbit_password_hashing_sha256;
                <<"3.5.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.4.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.3.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.2.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.1.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.0.", _/binary>> -> rabbit_password_hashing_md5;
                _                    -> rabbit_password:hashing_mod()
            end;
        Alg       -> binary_to_atom(Alg, utf8)
    end.
