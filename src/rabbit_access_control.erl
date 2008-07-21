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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_access_control).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-export([check_login/2, user_pass_login/2,
         check_vhost_access/2]).
-export([add_user/2, delete_user/1, change_password/2, list_users/0,
         lookup_user/1]).
-export([add_vhost/1, delete_vhost/1, list_vhosts/0, list_vhost_users/1]).
-export([list_user_vhosts/1, map_user_vhost/2, unmap_user_vhost/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(check_login/2 :: (binary(), binary()) -> user()).
-spec(user_pass_login/2 :: (username(), password()) -> user()).
-spec(check_vhost_access/2 :: (user(), vhost()) -> 'ok').
-spec(add_user/2 :: (username(), password()) -> 'ok').
-spec(delete_user/1 :: (username()) -> 'ok').
-spec(change_password/2 :: (username(), password()) -> 'ok').
-spec(list_users/0 :: () -> [username()]).
-spec(lookup_user/1 :: (username()) -> {'ok', user()} | not_found()).
-spec(add_vhost/1 :: (vhost()) -> 'ok').
-spec(delete_vhost/1 :: (vhost()) -> 'ok').
-spec(list_vhosts/0 :: () -> [vhost()]).
-spec(list_vhost_users/1 :: (vhost()) -> [username()]).
-spec(list_user_vhosts/1 :: (username()) -> [vhost()]).
-spec(map_user_vhost/2 :: (username(), vhost()) -> 'ok').
-spec(unmap_user_vhost/2 :: (username(), vhost()) -> 'ok').

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
              case mnesia:match_object(
                     #user_vhost{username = Username,
                                 virtual_host = VHostPath}) of
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
                    ok = mnesia:delete({user_vhost, Username})
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
                          #exchange{} = rabbit_exchange:declare(rabbit_misc:r(VHostPath,exchange,<<"">>), direct, true, false, []),
                          #exchange{} = rabbit_exchange:declare(rabbit_misc:r(VHostPath,exchange,<<"amq.direct">>), direct, true, false, []),
                          #exchange{} = rabbit_exchange:declare(rabbit_misc:r(VHostPath,exchange,<<"amq.topic">>), topic, true, false, []),
                          #exchange{} = rabbit_exchange:declare(rabbit_misc:r(VHostPath,exchange,<<"amq.fanout">>), fanout, true, false, []),
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
                  rabbit_amqqueue:list_vhost_queues(VHostPath)),
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
                  rabbit_exchange:list_vhost_exchanges(VHostPath)),
    lists:foreach(fun (Username) ->
                          ok = unmap_user_vhost(Username, VHostPath)
                  end,
                  list_vhost_users(VHostPath)),
    ok = mnesia:delete({vhost, VHostPath}),
    ok.

list_vhosts() ->
    mnesia:dirty_all_keys(vhost).

list_vhost_users(VHostPath) ->
    [Username ||
        #user_vhost{username = Username} <-
            %% TODO: use dirty ops instead
            rabbit_misc:execute_mnesia_transaction(
              rabbit_misc:with_vhost(
                VHostPath,
                fun () -> mnesia:index_read(user_vhost, VHostPath,
                                            #user_vhost.virtual_host)
                end))].

list_user_vhosts(Username) ->
    [VHostPath ||
        #user_vhost{virtual_host = VHostPath} <-
            %% TODO: use dirty ops instead
            rabbit_misc:execute_mnesia_transaction(
              rabbit_misc:with_user(
                Username,
                fun () -> mnesia:read({user_vhost, Username}) end))].

map_user_vhost(Username, VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                ok = mnesia:write(
                       #user_vhost{username = Username,
                                   virtual_host = VHostPath})
        end)).

unmap_user_vhost(Username, VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                ok = mnesia:delete_object(
                       #user_vhost{username = Username,
                                   virtual_host = VHostPath})
        end)).

