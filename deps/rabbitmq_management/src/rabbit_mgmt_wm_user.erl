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

-module(rabbit_mgmt_wm_user).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, user/1, put_user/2, put_user/3]).
-export([variances/2]).

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
    {case user(ReqData) of
         {ok, _}    -> true;
         {error, _} -> false
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    {ok, User} = user(ReqData),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:internal_user(User),
                           ReqData, Context).

accept_content(ReqData0, Context = #context{user = #user{username = ActingUser}}) ->
    Username = rabbit_mgmt_util:id(user, ReqData0),
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun(_, User, ReqData) ->
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
    Username        = maps:get(name, User),
    HasPassword     = maps:is_key(password, User),
    HasPasswordHash = maps:is_key(password_hash, User),
    Password        = maps:get(password, User, undefined),
    PasswordHash    = maps:get(password_hash, User, undefined),

    Tags            = case {maps:get(tags, User, undefined), maps:get(administrator, User, undefined)} of
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

    %% pre-configured, only applies to newly created users
    Permissions     = maps:get(permissions, User, undefined),

    PassedCredentialValidation =
        case {HasPassword, HasPasswordHash} of
            {true, false} ->
                rabbit_credential_validation:validate(Username, Password) =:= ok;
            {false, true} -> true;
            _             ->
                rabbit_credential_validation:validate(Username, Password) =:= ok
        end,

    case UserExists of
        true  ->
            case {HasPassword, HasPasswordHash} of
                {true, false} ->
                    update_user_password(PassedCredentialValidation, Username, Password, Tags, ActingUser);
                {false, true} ->
                    update_user_password_hash(Username, PasswordHash, Tags, User, Version, ActingUser);
                {true, true} ->
                    throw({error, both_password_and_password_hash_are_provided});
                %% clear password, update tags if needed
                _ ->
                    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser),
                    rabbit_auth_backend_internal:clear_password(Username, ActingUser)
            end;
        false ->
            case {HasPassword, HasPasswordHash} of
                {true, false}  ->
                    create_user_with_password(PassedCredentialValidation, Username, Password, Tags, Permissions, ActingUser);
                {false, true}  ->
                    create_user_with_password_hash(Username, PasswordHash, Tags, User, Version, Permissions, ActingUser);
                {true, true}   ->
                    throw({error, both_password_and_password_hash_are_provided});
                {false, false} ->
                    %% this user won't be able to sign in using
                    %% a username/password pair but can be used for x509 certificate authentication,
                    %% with authn backends such as HTTP or LDAP and so on.
                    create_user_with_password(PassedCredentialValidation, Username, <<"">>, Tags, Permissions, ActingUser)
            end
    end.

update_user_password(_PassedCredentialValidation = true,  Username, Password, Tags, ActingUser) ->
    rabbit_auth_backend_internal:change_password(Username, Password, ActingUser),
    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser);
update_user_password(_PassedCredentialValidation = false, _Username, _Password, _Tags, _ActingUser) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

update_user_password_hash(Username, PasswordHash, Tags, User, Version, ActingUser) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),

    Hash = rabbit_mgmt_util:b64decode_or_throw(PasswordHash),
    rabbit_auth_backend_internal:change_password_hash(
      Username, Hash, HashingAlgorithm),
    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser).

create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags, undefined, ActingUser) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser),
    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser);
create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags, PreconfiguredPermissions, ActingUser) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser),
    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser),
    preconfigure_permissions(Username, PreconfiguredPermissions, ActingUser);
create_user_with_password(_PassedCredentialValidation = false, _Username, _Password, _Tags, _, _) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

create_user_with_password_hash(Username, PasswordHash, Tags, User, Version, PreconfiguredPermissions, ActingUser) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),
    Hash             = rabbit_mgmt_util:b64decode_or_throw(PasswordHash),

    %% first we create a user with dummy credentials and no
    %% validation applied, then we update password hash
    TmpPassword = rabbit_guid:binary(rabbit_guid:gen_secure(), "tmp"),
    rabbit_auth_backend_internal:add_user_sans_validation(Username, TmpPassword, ActingUser),

    rabbit_auth_backend_internal:change_password_hash(
      Username, Hash, HashingAlgorithm),
    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser),
    preconfigure_permissions(Username, PreconfiguredPermissions, ActingUser).

preconfigure_permissions(_Username, undefined, _ActingUser) ->
    ok;
preconfigure_permissions(Username, Map, ActingUser) when is_map(Map) ->
    maps:map(fun(VHost, M) ->
                     rabbit_auth_backend_internal:set_permissions(Username, VHost,
                                                  maps:get(<<"configure">>, M),
                                                  maps:get(<<"write">>,     M),
                                                  maps:get(<<"read">>,      M),
                                                  ActingUser)
             end,
             Map),
    ok.

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
