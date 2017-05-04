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
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_user).

-export([init/3, rest_init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, user/1, put_user/1, put_user/2]).
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
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

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

accept_content(ReqData0, Context) ->
    Username = rabbit_mgmt_util:id(user, ReqData0),
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun(_, User, ReqData) ->
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

put_user(User) -> put_user(User, undefined).

put_user(User, Version) ->
    Username        = pget(name, User),
    HasPassword     = proplists:is_defined(password, User),
    HasPasswordHash = proplists:is_defined(password_hash, User),
    Password        = pget(password, User),
    PasswordHash    = pget(password_hash, User),

    Tags            = case {pget(tags, User), pget(administrator, User)} of
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

    UserExists      = case rabbit_auth_backend_internal:lookup_user(Username) of
                          %% expected
                          {error, not_found} -> false;
                          %% shouldn't normally happen but worth guarding
                          %% against
                          {error, _}         -> false;
                          _                  -> true
                      end,

    PassedCredentialValidation =
        case {HasPassword, HasPasswordHash} of
            {true, false} ->
                rabbit_credential_validation:validate(Username, Password) =:= ok;
            {false, true} -> true;
            _             -> false
        end,

    case UserExists of
        true  ->
            case {HasPassword, HasPasswordHash} of
                {true, false} ->
                    update_user_password(PassedCredentialValidation, Username, Password, Tags);
                {false, true} ->
                    update_user_password_hash(Username, PasswordHash, Tags, User, Version);
                {true, true} ->
                    throw({error, both_password_and_password_hash_are_provided});
                %% clears password
                _ ->
                    rabbit_auth_backend_internal:clear_password(Username)
            end;
        false ->
            case {HasPassword, HasPasswordHash} of
                {true, false}  ->
                    create_user_with_password(PassedCredentialValidation, Username, Password, Tags);
                {false, true}  ->
                    create_user_with_password_hash(Username, PasswordHash, Tags, User, Version);
                {true, true}   ->
                    throw({error, both_password_and_password_hash_are_provided});
                {false, false} ->
                    throw({error, no_password_or_password_hash_provided})
            end
    end.

update_user_password(_PassedCredentialValidation = true,  Username, Password, Tags) ->
    rabbit_auth_backend_internal:change_password(Username, Password),
    rabbit_auth_backend_internal:set_tags(Username, Tags);
update_user_password(_PassedCredentialValidation = false, _Username, _Password, _Tags) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

update_user_password_hash(Username, PasswordHash, Tags, User, Version) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),

    Hash = rabbit_mgmt_util:b64decode_or_throw(PasswordHash),
    rabbit_auth_backend_internal:change_password_hash(
      Username, Hash, HashingAlgorithm),
    rabbit_auth_backend_internal:set_tags(Username, Tags).

create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags) ->
    rabbit_auth_backend_internal:add_user(Username, Password),
    rabbit_auth_backend_internal:set_tags(Username, Tags);
create_user_with_password(_PassedCredentialValidation = false, _Username, _Password, _Tags) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

create_user_with_password_hash(Username, PasswordHash, Tags, User, Version) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),
    Hash             = rabbit_mgmt_util:b64decode_or_throw(PasswordHash),

    %% first we create a user with dummy credentials and no
    %% validation applied, then we update password hash
    TmpPassword = rabbit_guid:binary(rabbit_guid:gen_secure(), "tmp"),
    rabbit_auth_backend_internal:add_user_sans_validation(Username, TmpPassword),

    rabbit_auth_backend_internal:change_password_hash(
      Username, Hash, HashingAlgorithm),
    rabbit_auth_backend_internal:set_tags(Username, Tags).

hashing_algorithm(User, Version) ->
    case pget(hashing_algorithm, User) of
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
