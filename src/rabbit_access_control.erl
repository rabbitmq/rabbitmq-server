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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_access_control).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-export([check_login/2, user_pass_login/2,
         check_vhost_access/2, check_resource_access/3]).
-export([add_user/2, delete_user/1, change_password/2, list_users/0,
         lookup_user/1]).
-export([add_vhost/1, delete_vhost/1, list_vhosts/0]).
-export([set_permissions/4, clear_permissions/2,
         list_vhost_permissions/1, list_user_permissions/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(check_login/2 :: (binary(), binary()) -> user()).
-spec(user_pass_login/2 :: (username(), password()) -> user()).
-spec(check_vhost_access/2 :: (user(), vhost()) -> 'ok').
-spec(check_resource_access/3 ::
      (username(), r(atom()), non_neg_integer()) -> 'ok').
-spec(add_user/2 :: (username(), password()) -> 'ok').
-spec(delete_user/1 :: (username()) -> 'ok').
-spec(change_password/2 :: (username(), password()) -> 'ok').
-spec(list_users/0 :: () -> [username()]).
-spec(lookup_user/1 :: (username()) -> {'ok', user()} | not_found()).
-spec(add_vhost/1 :: (vhost()) -> 'ok').
-spec(delete_vhost/1 :: (vhost()) -> 'ok').
-spec(list_vhosts/0 :: () -> [vhost()]).
-spec(set_permissions/4 :: (username(), vhost(), regexp(), regexp()) -> 'ok').
-spec(clear_permissions/2 :: (username(), vhost()) -> 'ok').
-spec(list_vhost_permissions/1 ::
      (vhost()) -> [{username(), regexp(), regexp()}]).
-spec(list_user_permissions/1 ::
      (username()) -> [{vhost(), regexp(), regexp()}]).

-endif.

%%----------------------------------------------------------------------------

%% SASL PLAIN, as used by the Qpid Java client and our clients. Also,
%% apparently, by OpenAMQ.
check_login(<<"PLAIN">>, Response) ->
    [User, Pass] = [list_to_binary(T) ||
                       T <- string:tokens(binary_to_list(Response), [0])],
    user_pass_login(User, Pass);
%% AMQPLAIN, as used by Qpid Python test suite. The 0-8 spec actually
%% defines this as PLAIN, but in 0-9 that definition is gone, instead
%% referring generically to "SASL security mechanism", i.e. the above.
check_login(<<"AMQPLAIN">>, Response) ->
    LoginTable = rabbit_binary_parser:parse_table(Response),
    case {lists:keysearch(<<"LOGIN">>, 1, LoginTable),
          lists:keysearch(<<"PASSWORD">>, 1, LoginTable)} of
        {{value, {_, longstr, User}},
         {value, {_, longstr, Pass}}} ->
            user_pass_login(User, Pass);
        _ ->
            %% Is this an information leak?
            rabbit_misc:protocol_error(
              access_refused,
              "AMQPPLAIN auth info ~w is missing LOGIN or PASSWORD field",
              [LoginTable])
    end;

check_login(Mechanism, _Response) ->
    rabbit_misc:protocol_error(
      access_refused, "unsupported authentication mechanism '~s'",
      [Mechanism]).

user_pass_login(User, Pass) ->
    ?LOGDEBUG("Login with user ~p pass ~p~n", [User, Pass]),
    case lookup_user(User) of
        {ok, U} ->
            if
                Pass == U#user.password -> U;
                true ->
                    rabbit_misc:protocol_error(
                      access_refused, "login refused for user '~s'", [User])
            end;
        {error, not_found} ->
            rabbit_misc:protocol_error(
              access_refused, "login refused for user '~s'", [User])
    end.

internal_lookup_vhost_access(Username, VHostPath) ->
    %% TODO: use dirty ops instead
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read({user_permission,
                                #user_vhost{username = Username,
                                            virtual_host = VHostPath}}) of
                  [] -> not_found;
                  [R] -> {ok, R}
              end
      end).

check_vhost_access(#user{username = Username}, VHostPath) ->
    ?LOGDEBUG("Checking VHost access for ~p to ~p~n", [Username, VHostPath]),
    case internal_lookup_vhost_access(Username, VHostPath) of
        {ok, _R} ->
            ok;
        not_found ->
            rabbit_misc:protocol_error(
              access_refused, "access to vhost '~s' refused for user '~s'",
              [VHostPath, Username])
    end.

check_resource_access(Username,
                      R = #resource{kind = exchange, name = <<"">>},
                      Permission) ->
    check_resource_access(Username,
                          R#resource{name = <<"amq.default">>},
                          Permission);
check_resource_access(_Username,
                      #resource{name = <<"amq.gen",_/binary>>},
                      _Permission) ->
    ok;
check_resource_access(Username,
                      R = #resource{virtual_host = VHostPath, name = Name},
                      Permission) ->
    Res = case mnesia:dirty_read({user_permission,
                                  #user_vhost{username = Username,
                                              virtual_host = VHostPath}}) of
              [] ->
                  false;
              [#user_permission{permission = P}] ->
                  case regexp:match(
                         binary_to_list(Name),
                         binary_to_list(element(Permission, P))) of
                      {match, _, _} -> true;
                      nomatch       -> false
                  end
          end,
    if Res  -> ok;
       true -> rabbit_misc:protocol_error(
                 access_refused, "access to ~s refused for user '~s'",
                 [rabbit_misc:rs(R), Username])
    end.

add_user(Username, Password) ->
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:read({user, Username}) of
                      [] ->
                          ok = mnesia:write(#user{username = Username,
                                                  password = Password});
                      _ ->
                          mnesia:abort({user_already_exists, Username})
                  end
          end),
    rabbit_log:info("Created user ~p~n", [Username]),
    R.

delete_user(Username) ->
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_user(
            Username,
            fun () ->
                    ok = mnesia:delete({user, Username}),
                    [ok = mnesia:delete_object(R) ||
                        R <- mnesia:match_object(
                               #user_permission{user_vhost = #user_vhost{
                                                  username = Username,
                                                  virtual_host = '_'},
                                                permission = '_'})],
                    ok
            end)),
    rabbit_log:info("Deleted user ~p~n", [Username]),
    R.

change_password(Username, Password) ->
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_user(
            Username,
            fun () ->
                    ok = mnesia:write(#user{username = Username,
                                            password = Password})
            end)),
    rabbit_log:info("Changed password for user ~p~n", [Username]),
    R.

list_users() ->
    mnesia:dirty_all_keys(user).

lookup_user(Username) ->
    rabbit_misc:dirty_read({user, Username}).

add_vhost(VHostPath) ->
    R = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:read({vhost, VHostPath}) of
                      [] ->
                          ok = mnesia:write(#vhost{virtual_host = VHostPath}),
                          [rabbit_exchange:declare(
                             rabbit_misc:r(VHostPath, exchange, Name),
                             Type, true, false, []) ||
                              {Name,Type} <-
                                  [{<<"">>,           direct},
                                   {<<"amq.direct">>, direct},
                                   {<<"amq.topic">>,  topic},
				   {<<"amq.match">>,  headers}, %% per 0-9-1 pdf
				   {<<"amq.headers">>,  headers}, %% per 0-9-1 xml
                                   {<<"amq.fanout">>, fanout}]],
                          ok;
                      [_] ->
                          mnesia:abort({vhost_already_exists, VHostPath})
                  end
          end),
    rabbit_log:info("Added vhost ~p~n", [VHostPath]),
    R.

delete_vhost(VHostPath) ->
    %%FIXME: We are forced to delete the queues outside the TX below
    %%because queue deletion involves sending messages to the queue
    %%process, which in turn results in further mnesia actions and
    %%eventually the termination of that process.
    lists:foreach(fun (Q) ->
                          {ok,_} = rabbit_amqqueue:delete(Q, false, false)
                  end,
                  rabbit_amqqueue:list(VHostPath)),
    R = rabbit_misc:execute_mnesia_transaction(
          rabbit_misc:with_vhost(
            VHostPath,
            fun () ->
                    ok = internal_delete_vhost(VHostPath)
            end)),
    rabbit_log:info("Deleted vhost ~p~n", [VHostPath]),
    R.

internal_delete_vhost(VHostPath) ->
    lists:foreach(fun (#exchange{name=Name}) ->
                          ok = rabbit_exchange:delete(Name, false)
                  end,
                  rabbit_exchange:list(VHostPath)),
    lists:foreach(fun ({Username, _, _}) ->
                          ok = clear_permissions(Username, VHostPath)
                  end,
                  list_vhost_permissions(VHostPath)),
    ok = mnesia:delete({vhost, VHostPath}),
    ok.

list_vhosts() ->
    mnesia:dirty_all_keys(vhost).

validate_regexp(RegexpBin) ->
    Regexp = binary_to_list(RegexpBin),
    case regexp:parse(Regexp) of
        {ok, _}         -> ok;
        {error, Reason} -> throw({error, {invalid_regexp, Regexp, Reason}})
    end.

set_permissions(Username, VHostPath, ConfigurationPerm, MessagingPerm) ->
    validate_regexp(ConfigurationPerm),
    validate_regexp(MessagingPerm),
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () -> ok = mnesia:write(
                         #user_permission{user_vhost = #user_vhost{
                                            username = Username,
                                            virtual_host = VHostPath},
                                          permission = #permission{
                                            configuration = ConfigurationPerm,
                                            messaging = MessagingPerm}})
        end)).

clear_permissions(Username, VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                ok = mnesia:delete({user_permission,
                                    #user_vhost{username = Username,
                                                virtual_host = VHostPath}})
        end)).

list_vhost_permissions(VHostPath) ->
    [{Username, ConfigurationPerm, MessagingPerm} ||
        {Username, _, ConfigurationPerm, MessagingPerm} <-
            list_permissions(rabbit_misc:with_vhost(
                               VHostPath, match_user_vhost('_', VHostPath)))].

list_user_permissions(Username) ->
    [{VHostPath, ConfigurationPerm, MessagingPerm} ||
        {_, VHostPath, ConfigurationPerm, MessagingPerm} <-
            list_permissions(rabbit_misc:with_user(
                               Username, match_user_vhost(Username, '_')))].

list_permissions(QueryThunk) ->
    [{Username, VHostPath, ConfigurationPerm, MessagingPerm} ||
        #user_permission{user_vhost = #user_vhost{username = Username,
                                                  virtual_host = VHostPath},
                         permission = #permission{
                           configuration = ConfigurationPerm,
                           messaging = MessagingPerm}} <-
            %% TODO: use dirty ops instead
            rabbit_misc:execute_mnesia_transaction(QueryThunk)].

match_user_vhost(Username, VHostPath) ->
    fun () -> mnesia:match_object(
                #user_permission{user_vhost = #user_vhost{
                                   username = Username,
                                   virtual_host = VHostPath},
                                 permission = '_'})
    end.
