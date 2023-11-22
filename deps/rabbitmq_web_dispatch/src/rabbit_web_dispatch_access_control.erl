%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_web_dispatch_access_control).

-include("rabbitmq_web_dispatch_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([is_authorized/3, is_authorized/7, is_authorized_admin/3,
         is_authorized_admin/5, vhost/1, vhost_from_headers/1]).
-export([is_authorized_vhost/3, is_authorized_user/4,
         is_authorized_user/5, is_authorized_user/6,
         is_authorized_monitor/3, is_authorized_policies/3,
         is_authorized_vhost_visible/3,
         is_authorized_vhost_visible_for_monitoring/3,
         is_authorized_global_parameters/3]).

-export([list_visible_vhosts/1, list_visible_vhosts_names/1, list_login_vhosts/2]).
-export([id/2]).
-export([not_authorised/3, halt_response/5]).

-export([is_admin/1, is_policymaker/1, is_monitor/1, is_mgmt_user/1]).

-import(rabbit_misc, [pget/2]).

is_authorized(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context, '', fun(_) -> true end, AuthConfig).

is_authorized_admin(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"Not administrator user">>,
                  fun(#user{tags = Tags}) -> is_admin(Tags) end, AuthConfig).

is_authorized_admin(ReqData, Context, Username, Password, AuthConfig) ->
    case is_basic_auth_disabled(AuthConfig) of
        true ->
            Msg = "HTTP access denied: basic auth disabled",
            rabbit_log:warning(Msg),
            not_authorised(Msg, ReqData, Context);
        false ->
            is_authorized(ReqData, Context, Username, Password,
                          <<"Not administrator user">>,
                          fun(#user{tags = Tags}) -> is_admin(Tags) end, AuthConfig)
    end.

is_authorized_monitor(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"Not monitor user">>,
                  fun(#user{tags = Tags}) -> is_monitor(Tags) end,
                  AuthConfig).

is_authorized_vhost(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access virtual host">>,
                  fun(#user{tags = Tags} = User) ->
                          is_admin(Tags) orelse user_matches_vhost(ReqData, User)
                  end,
                  AuthConfig).

is_authorized_vhost_visible(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access virtual host">>,
                  fun(#user{tags = Tags} = User) ->
                          is_admin(Tags) orelse user_matches_vhost_visible(ReqData, User)
                  end,
                  AuthConfig).

is_authorized_vhost_visible_for_monitoring(ReqData, Context, AuthConfig) ->
  is_authorized(ReqData, Context,
                <<"User not authorised to access virtual host">>,
                fun(#user{tags = Tags} = User) ->
                        is_admin(Tags)
                            orelse is_monitor(Tags)
                            orelse user_matches_vhost_visible(ReqData, User)
                end,
                AuthConfig).

is_authorized(ReqData, Context, ErrorMsg, Fun, AuthConfig) ->
    case cowboy_req:method(ReqData) of
        <<"OPTIONS">> -> {true, ReqData, Context};
        _             -> is_authorized1(ReqData, Context, ErrorMsg, Fun, AuthConfig)
    end.

is_authorized1(ReqData, Context, ErrorMsg, Fun, AuthConfig) ->
    case cowboy_req:parse_header(<<"authorization">>, ReqData) of
        {basic, Username, Password} ->
            case is_basic_auth_disabled(AuthConfig) of
                true ->
                    Msg = "HTTP access denied: basic auth disabled",
                    rabbit_log:warning(Msg),
                    not_authorised(Msg, ReqData, Context);
                false ->
                    is_authorized(ReqData, Context,
                                  Username, Password,
                                  ErrorMsg, Fun, AuthConfig)
            end;
        {bearer, Token} ->
            % Username is only used in case is_authorized is not able to parse the token
            % and extact the username from it
            Username = AuthConfig#auth_settings.oauth_client_id,
            is_authorized(ReqData, Context, Username, Token, ErrorMsg, Fun, AuthConfig);
        _ ->
            case is_basic_auth_disabled(AuthConfig) of
                true ->
                    Msg = "HTTP access denied: basic auth disabled",
                    rabbit_log:warning(Msg),
                    not_authorised(Msg, ReqData, Context);
                false ->
                    {{false, AuthConfig#auth_settings.auth_realm}, ReqData, Context}
            end
    end.

is_authorized_user(ReqData, Context, Username, Password, AuthConfig) ->
    Msg = <<"User not authorized">>,
    Fun = fun(_) -> true end,
    is_authorized(ReqData, Context, Username, Password, Msg, Fun, AuthConfig).

is_authorized_user(ReqData, Context, Username, Password, ReplyWhenFailed, AuthConfig) ->
    Msg = <<"User not authorized">>,
    Fun = fun(_) -> true end,
    is_authorized(ReqData, Context, Username, Password, Msg, Fun, AuthConfig, ReplyWhenFailed).

is_authorized(ReqData, Context, Username, Password, ErrorMsg, Fun, AuthConfig) ->
    is_authorized(ReqData, Context, Username, Password, ErrorMsg, Fun, AuthConfig, true).

is_authorized(ReqData, Context, Username, Password, ErrorMsg, Fun, AuthConfig, ReplyWhenFailed) ->
    ErrFun = fun (ResolvedUserName, Msg) ->
                     rabbit_log:warning("HTTP access denied: user '~ts' - ~ts",
                                        [ResolvedUserName, Msg]),
                     case ReplyWhenFailed of
                       true -> not_authorised(Msg, ReqData, Context);
                       false -> {false, ReqData, "Not_Authorized"}
                     end
             end,
    AuthProps = [{password, Password}] ++ case vhost(ReqData) of
        VHost when is_binary(VHost) -> [{vhost, VHost}];
        _                           -> []
    end,
    {IP, _} = cowboy_req:peer(ReqData),
    case rabbit_access_control:check_user_login(Username, AuthProps) of
        {ok, User = #user{username = ResolvedUsername, tags = Tags}} ->
            case rabbit_access_control:check_user_loopback(ResolvedUsername, IP) of
                ok ->
                    case is_mgmt_user(Tags) of
                        true ->
                            case Fun(User) of
                                true  ->
                                    rabbit_core_metrics:auth_attempt_succeeded(IP, ResolvedUsername, http),
                                    {true, ReqData,
                                     Context#context{user     = User,
                                                     password = Password}};
                                false ->
                                    rabbit_core_metrics:auth_attempt_failed(IP, ResolvedUsername, http),
                                    ErrFun(ResolvedUsername, ErrorMsg)
                            end;
                        false ->
                            rabbit_core_metrics:auth_attempt_failed(IP, ResolvedUsername, http),
                            ErrFun(ResolvedUsername, <<"Not management user">>)
                    end;
                not_allowed ->
                    rabbit_core_metrics:auth_attempt_failed(IP, ResolvedUsername, http),
                    ErrFun(ResolvedUsername, <<"User can only log in via localhost">>)
            end;
        {refused, _Username, Msg, Args} ->
            rabbit_core_metrics:auth_attempt_failed(IP, Username, http),
            rabbit_log:warning("HTTP access denied: ~ts",
                               [rabbit_misc:format(Msg, Args)]),
            case ReplyWhenFailed of
              true -> not_authenticated(<<"Not_Authorized">>, ReqData, Context, AuthConfig);
              false -> {false, ReqData, "Not_Authorized"}
            end
    end.


%% Used for connections / channels. A normal user can only see / delete
%% their own stuff. Monitors can see other users' and delete their
%% own. Admins can do it all.
is_authorized_user(ReqData, Context, Item, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(#user{username = Username, tags = Tags}) ->
                          case cowboy_req:method(ReqData) of
                              <<"DELETE">> -> is_admin(Tags);
                              _            -> is_monitor(Tags)
                          end orelse Username == pget(user, Item)
                  end,
                  AuthConfig).

%% For policies / parameters. Like is_authorized_vhost but you have to
%% be a policymaker.
is_authorized_policies(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(User = #user{tags = Tags}) ->
                          is_admin(Tags) orelse
                                           (is_policymaker(Tags) andalso
                                            user_matches_vhost(ReqData, User))
                  end,
                  AuthConfig).

%% For global parameters. Must be policymaker.
is_authorized_global_parameters(ReqData, Context, AuthConfig) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(#user{tags = Tags}) ->
                           is_policymaker(Tags)
                  end,
                  AuthConfig).

vhost_from_headers(ReqData) ->
    case cowboy_req:header(<<"x-vhost">>, ReqData) of
        undefined -> none;
        %% blank x-vhost means "All hosts" is selected in the UI
        <<>>      -> none;
        VHost     -> VHost
    end.

vhost(ReqData) ->
    Value = case id(vhost, ReqData) of
      none  -> vhost_from_headers(ReqData);
      VHost -> VHost
    end,
    case Value of
      none -> none;
      Name ->
        case rabbit_vhost:exists(Name) of
          true  -> Name;
          false -> not_found
        end
    end.

is_admin(T)       -> intersects(T, [administrator]).
is_policymaker(T) -> intersects(T, [administrator, policymaker]).
is_monitor(T)     -> intersects(T, [administrator, monitoring]).
is_mgmt_user(T)   -> intersects(T, [administrator, monitoring, policymaker,
                                    management]).

intersects(A, B) -> lists:any(fun(I) -> lists:member(I, B) end, A).

user_matches_vhost(ReqData, User) ->
    case vhost(ReqData) of
        not_found -> true;
        none      -> true;
        V         ->
            AuthzData = get_authz_data(ReqData),
            lists:member(V, list_login_vhosts_names(User, AuthzData))
    end.

user_matches_vhost_visible(ReqData, User) ->
    case vhost(ReqData) of
        not_found -> true;
        none      -> true;
        V         ->
            AuthzData = get_authz_data(ReqData),
            lists:member(V, list_visible_vhosts_names(User, AuthzData))
    end.

get_authz_data(ReqData) ->
    {PeerAddress, _PeerPort} = cowboy_req:peer(ReqData),
    {ip, PeerAddress}.


not_authorised(Reason, ReqData, Context) ->
    %% TODO: consider changing to 403 in 4.0
    halt_response(401, not_authorised, Reason, ReqData, Context).

halt_response(Code, Type, Reason, ReqData, Context) ->
    ReasonFormatted = format_reason(Reason),
    Json = #{<<"error">>  => Type,
             <<"reason">> => ReasonFormatted},
    ReqData1 = cowboy_req:reply(Code,
        #{<<"content-type">> => <<"application/json">>},
        rabbit_json:encode(Json), ReqData),
    {stop, ReqData1, Context}.

not_authenticated(Reason, ReqData, Context,
                  #auth_settings{auth_realm = AuthRealm} = AuthConfig) ->
    case is_oauth2_enabled(AuthConfig) of
      false ->
            ReqData1 = cowboy_req:set_resp_header(<<"www-authenticate">>, AuthRealm, ReqData),
            halt_response(401, not_authorized, Reason, ReqData1, Context);
       true ->
            halt_response(401, not_authorized, Reason, ReqData, Context)
    end.

format_reason(Tuple) when is_tuple(Tuple) ->
    tuple(Tuple);
format_reason(Binary) when is_binary(Binary) ->
    Binary;
format_reason(Other) ->
    case is_string(Other) of
        true ->  print("~ts", [Other]);
        false -> print("~tp", [Other])
    end.

print(Fmt, Val) when is_list(Val) ->
    list_to_binary(lists:flatten(io_lib:format(Fmt, Val))).

is_string(List) when is_list(List) ->
    lists:all(
        fun(El) -> is_integer(El) andalso El > 0 andalso El < 16#10ffff end,
        List);
is_string(_) -> false.

tuple(unknown)                    -> unknown;
tuple(Tuple) when is_tuple(Tuple) -> [tuple(E) || E <- tuple_to_list(Tuple)];
tuple(Term)                       -> Term.

id(Key, ReqData) when Key =:= exchange;
                      Key =:= source;
                      Key =:= destination ->
    case id0(Key, ReqData) of
        <<"amq.default">> -> <<"">>;
        Name              -> Name
    end;
id(Key, ReqData) ->
    id0(Key, ReqData).

id0(Key, ReqData) ->
    case cowboy_req:binding(Key, ReqData) of
        undefined -> none;
        Id        -> Id
    end.


list_visible_vhosts_names(User) ->
    list_visible_vhosts(User, undefined).

list_visible_vhosts_names(User, AuthzData) ->
    list_visible_vhosts(User, AuthzData).

list_visible_vhosts(User) ->
    list_visible_vhosts(User, undefined).

list_visible_vhosts(User = #user{tags = Tags}, AuthzData) ->
    case is_monitor(Tags) of
        true  -> rabbit_vhost:list_names();
        false -> list_login_vhosts_names(User, AuthzData)
    end.

list_login_vhosts_names(User, AuthzData) ->
    [V || V <- rabbit_vhost:list_names(),
          case catch rabbit_access_control:check_vhost_access(User, V, AuthzData, #{}) of
              ok -> true;
              NotOK ->
                  log_access_control_result(NotOK),
                  false
          end].

list_login_vhosts(User, AuthzData) ->
    [V || V <- rabbit_vhost:all(),
          case catch rabbit_access_control:check_vhost_access(User, vhost:get_name(V), AuthzData, #{}) of
              ok -> true;
              NotOK ->
                  log_access_control_result(NotOK),
                  false
          end].

% rabbitmq/rabbitmq-auth-backend-http#100
log_access_control_result(NotOK) ->
    rabbit_log:debug("rabbit_access_control:check_vhost_access result: ~tp", [NotOK]).

is_basic_auth_disabled(#auth_settings{basic_auth_enabled = Enabled}) ->
    not Enabled.

is_oauth2_enabled(#auth_settings{oauth2_enabled = Enabled}) ->
    Enabled.
