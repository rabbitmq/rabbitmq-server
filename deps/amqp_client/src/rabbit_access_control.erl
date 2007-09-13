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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
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
         check_vhost_access/2, lookup_realm_access/2]).
-export([add_user/2, delete_user/1, change_password/2, list_users/0,
         lookup_user/1]).
-export([add_vhost/1, delete_vhost/1, list_vhosts/0, list_vhost_users/1]).
-export([list_user_vhosts/1, map_user_vhost/2, unmap_user_vhost/2]).
-export([list_user_realms/2, map_user_realm/2, full_ticket/1]).

%% SASL PLAIN, as used by the Qpid Java client and our clients. Also,
%% apparently, by OpenAMQ.
check_login(<<"PLAIN">>, Response) ->
    [User, Pass] = lists:map(fun list_to_binary/1,
                             string:tokens(binary_to_list(Response), [0])),
    {ok, _U} = user_pass_login(User, Pass);

%% AMQPLAIN, as used by Qpid Python test suite. The 0-8 spec actually
%% defines this as PLAIN, but in 0-9 that definition is gone, instead
%% referring generically to "SASL security mechanism", i.e. the above.
check_login(<<"AMQPLAIN">>, Response) ->
    LoginTable = rabbit_binary_parser:parse_table(Response),
    case {lists:keysearch(<<"LOGIN">>, 1, LoginTable),
          lists:keysearch(<<"PASSWORD">>, 1, LoginTable)} of
        {{value, {_, longstr, User}},
         {value, {_, longstr, Pass}}} ->
            {ok, _U} = user_pass_login(User, Pass);
        _ ->
            rabbit_misc:die(access_refused)
    end;

check_login(Mechanism, _Response) -> 	 
    rabbit_log:error("Unsupported authentication mechanism ~p~n",
                      [Mechanism]), 	 
    rabbit_misc:die(access_refused).

user_pass_login(User, Pass) ->
    ?LOGDEBUG("Login with user ~p pass ~p~n", [User, Pass]),
    case lookup_user(User) of
        none ->
            rabbit_log:info("Login refused: user ~p not found~n", [User]),
            rabbit_misc:die(access_refused);
        {ok, U} ->
            if
                Pass == U#user.password -> {ok, U};
                true ->
                    rabbit_log:info("Login refused: incorrect password for ~p~n", [User]),
                    rabbit_misc:die(access_refused)
            end
    end.

internal_lookup_vhost_access(Username, VHostPath) ->
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  case mnesia:match_object(
                         #user_vhost{username = Username,
                                     virtual_host = VHostPath}) of
                      [] -> not_found;
                      [R] -> {ok, R}
                  end
          end),
    Result.

check_vhost_access(#user{username = Username}, VHostPath) ->
    ?LOGDEBUG("Checking VHost access for ~p to ~p~n", [Username, VHostPath]),
    case internal_lookup_vhost_access(Username, VHostPath) of
        {ok, _R} ->
            ok;
        not_found ->
            rabbit_log:info("VHost access refused: no mapping for User ~p, VHost ~p~n",
                            [Username, VHostPath]),
            rabbit_misc:die(access_refused)
    end.

lookup_realm_access(#user{username = Username}, RealmName = #resource{kind = realm}) ->
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  case user_realms(Username, RealmName) of
                      [] ->
                          none;
                      [#user_realm{ticket_pattern = TicketPattern}] ->
                          TicketPattern
                  end
          end),
    Result.

add_user(Username, Password) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read({user, Username}) of
                  [] ->
                      ok = mnesia:write(#user{username = Username,
                                              password = Password});
                  _ ->
                      mnesia:abort({user_already_exists, Username})
              end
      end,
      fun (Result) ->
              rabbit_log:info("Created user ~p~n", [Username]),
              Result
      end).

delete_user(Username) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user(
        Username,
        fun () ->
                ok = mnesia:delete({user, Username}),
                ok = mnesia:delete({user_vhost, Username}),
                ok = mnesia:delete({user_realm, Username})
        end),
      fun (Result) ->
              rabbit_log:info("Deleted user ~p~n", [Username]),
              Result
      end).

change_password(Username, Password) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user(
        Username,
        fun () ->
                ok = mnesia:write(#user{username = Username,
                                        password = Password})
        end),
      fun (Result) ->
              rabbit_log:info("Changed password for user ~p~n", [Username]),
              Result
      end).

list_users() ->
    {atomic, Usernames} =
        mnesia:transaction(fun () -> mnesia:all_keys(user) end),
    {ok, Usernames}.

lookup_user(Username) ->
    case rabbit_misc:clean_read({user, Username}) of
        {ok, U} -> {ok, U};
        {error, not_found} -> none
    end.

add_vhost(VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read({vhost, VHostPath}) of
                  [] ->
                      ok = mnesia:write(#vhost{virtual_host = VHostPath}),
                      DataRealm =
                          rabbit_misc:r(VHostPath, realm, <<"/data">>),
                           AdminRealm =
                          rabbit_misc:r(VHostPath, realm, <<"/admin">>),
                      ok = rabbit_realm:add_realm(DataRealm),
                           ok = rabbit_realm:add_realm(AdminRealm),
                      #exchange{} = rabbit_exchange:declare(
                                      DataRealm, <<"">>,
                                      direct, true, false, []),
                      #exchange{} = rabbit_exchange:declare(
                                      DataRealm, <<"amq.direct">>,
                                      direct, true, false, []),
                      #exchange{} = rabbit_exchange:declare(
                                      DataRealm, <<"amq.topic">>,
                                      topic, true, false, []),
                      #exchange{} = rabbit_exchange:declare(
                                      DataRealm, <<"amq.fanout">>,
                                      fanout, true, false, []),
                      ok;
                  [_] ->
                      mnesia:abort({vhost_already_exists, VHostPath})
              end
      end,
      fun (Result) ->
              rabbit_log:info("Added vhost ~p~n", [VHostPath]),
              Result
      end).

delete_vhost(VHostPath) ->
    %%FIXME: We are forced to delete the queues outside the TX below
    %%because queue deletion involves sending messages to the queue
    %%process, which in turn results in further mnesia actions and
    %%eventually the termination of that process.
    lists:foreach(fun (Q) ->
                          {ok,_} = rabbit_amqqueue:delete(Q, false, false)
                  end,
                  rabbit_amqqueue:list_vhost_queues(VHostPath)),
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_vhost(
        VHostPath,
        fun () ->
                ok = internal_delete_vhost(VHostPath)
        end),
      fun (Result) ->
              rabbit_log:info("Deleted vhost ~p~n", [VHostPath]),
              Result
      end).

internal_delete_vhost(VHostPath) ->
    Exchanges        = rabbit_exchange:list_vhost_exchanges(VHostPath),
    {ok, Realms}     = rabbit_realm:list_vhost_realms(VHostPath),
    {ok, Usernames}  = list_vhost_users(VHostPath),
    lists:foreach(fun (#exchange{name=Name}) ->
                          ok = rabbit_exchange:delete(Name, false)
                  end,
                  Exchanges),
    lists:foreach(fun (RealmName) ->
                          ok = rabbit_realm:delete_realm(
                                 rabbit_misc:r(VHostPath, realm, RealmName))
                  end,
                  Realms),
    lists:foreach(fun (Username) ->
                          ok = unmap_user_vhost(Username, VHostPath)
                  end,
                  Usernames),
    ok = mnesia:delete({vhost, VHostPath}),
    ok.

list_vhosts() ->
    {atomic, VHosts} =
        mnesia:transaction(fun () -> mnesia:all_keys(vhost) end),
    {ok, VHosts}.

list_vhost_users(VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_vhost(
        VHostPath,
        fun () ->
                mnesia:index_read(user_vhost, VHostPath,
                                  #user_vhost.virtual_host)
        end),
      fun (Result) ->
              {ok, lists:map(fun (#user_vhost{username = Username}) ->
                                     Username
                             end, Result)}
      end).
    
list_user_vhosts(Username) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user(
        Username,
        fun () ->
                mnesia:read({user_vhost, Username})
        end),
      fun (Result) ->
              {ok, lists:map(fun (#user_vhost{virtual_host = VHostPath}) ->
                                     VHostPath
                             end, Result)}
      end).

map_user_vhost(Username, VHostPath) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                ok = mnesia:write(
                       #user_vhost{username = Username,
                                   virtual_host = VHostPath})
        end)).

unmap_user_vhost(Username, VHostPath) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                lists:foreach(fun mnesia:delete_object/1,
                              user_realms(Username,
                                          rabbit_misc:r(VHostPath, realm))),
                ok = mnesia:delete_object(
                       #user_vhost{username = Username,
                                   virtual_host = VHostPath})
        end)).

map_user_realm(Username,
               Ticket = #ticket{realm_name = RealmName =
                                #resource{virtual_host = VHostPath,
                                          kind = realm}}) ->
    rabbit_misc:execute_simple_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        rabbit_misc:with_realm(
          RealmName,
          fun () ->
                  lists:foreach(fun mnesia:delete_object/1,
                                user_realms(Username, RealmName)),
                  case internal_lookup_vhost_access(Username, VHostPath) of
                      {ok, _R} ->
                          case ticket_liveness(Ticket) of
                              alive ->
                                  ok = mnesia:write(
                                         #user_realm{username = Username,
                                                     realm = RealmName,
                                                     ticket_pattern = Ticket});
                              dead ->
                                  ok
                          end;
                      not_found ->
                          mnesia:abort(not_mapped_to_vhost)
                  end
          end))).

list_user_realms(Username, VHostPath) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_misc:with_user_and_vhost(
        Username, VHostPath,
        fun () ->
                case internal_lookup_vhost_access(Username, VHostPath) of
                    {ok, _R} ->
                        user_realms(Username,
                                    rabbit_misc:r(VHostPath, realm));
                    not_found ->
                        mnesia:abort(not_mapped_to_vhost)
                end
        end),
      fun (Result) ->
              {ok, lists:map(fun (#user_realm{realm = #resource{name = Name},
                                              ticket_pattern = Pattern}) ->
                                     {Name, Pattern}
                             end,
                             Result)}
      end).

ticket_liveness(#ticket{passive_flag = false,
                        active_flag = false,
                        write_flag = false,
                        read_flag = false}) ->
    dead;
ticket_liveness(_) ->
    alive.

full_ticket(RealmName) ->
    #ticket{realm_name = RealmName,
            passive_flag = true,
            active_flag = true,
            write_flag = true,
            read_flag = true}.

user_realms(Username, RealmName) ->
    mnesia:match_object(#user_realm{username = Username,
                                    realm = RealmName,
                                    _ = '_'}).
