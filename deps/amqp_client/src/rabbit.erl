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

-module(rabbit).

-behaviour(application).

-export([start/2, stop/1]).
-export([start/0, stop/0, stop_and_halt/0, status/0]).

-import(application).
-import(mnesia).
-import(lists).
-import(inet).
-import(gen_tcp).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

%%--------------------------------------------------------------------

start() ->
    try
        ok = ensure_working_log_config(),
        rabbit_misc:ensure_ok(
          case application:start(os_mon) of
              ok -> ok;
              {error, {already_started, _}} -> ok;
              Other -> Other
          end,
          cannot_start_os_mon),
        case mnesia:system_info(is_running) of
            yes -> ok;
            no -> rabbit_misc:ensure_ok(rabbit_mnesia:start(),
                                        cannot_start_mnesia)
        end,
        rabbit_misc:ensure_ok(application:start(rabbit), cannot_start_rabbit),
        ok
    after
        %%give the error loggers some time to catch up
        timer:sleep(100)
    end.

stop() ->
    rabbit_misc:ensure_ok(application:stop(rabbit), cannot_stop_rabbit),
    rabbit_misc:ensure_ok(rabbit_mnesia:stop(), cannot_stop_mnesia),
    rabbit_misc:ensure_ok(application:stop(os_mon), cannot_stop_os_mon),
    ok.

stop_and_halt() ->
    spawn(fun () ->
                  SleepTime = 1000,
                  rabbit_log:info("Stop-and-halt request received; halting in ~p milliseconds~n",
                                  [SleepTime]),
                  timer:sleep(SleepTime),
                  halt(0)
          end),
    case catch stop() of _ -> ok end.

status() ->
    [{running_applications, application:which_applications()}] ++
        rabbit_mnesia:status().

%%--------------------------------------------------------------------

start(normal, []) ->

    {ok, SupPid} = rabbit_sup:start_link(),

    print_banner(),

    lists:foreach(
      fun ({Msg, Thunk}) ->
              io:format("starting ~-20s ...", [Msg]),
              Thunk(),
              io:format("done~n")
      end,
      [{"database",
        fun () -> ok = rabbit_mnesia:init() end},
       {"core processes",
        fun () ->
                ok = start_child(rabbit_log),
                ok = start_child(rabbit_gensym),
                
                ok = rabbit_binary_generator:
                    check_empty_content_body_frame_size(),
                
                ok = start_child(rabbit_node_monitor),
                ok = start_child(rabbit_amqqueue),
                ok = start_child(rabbit_realm)
        end},
       {"recovery",
        fun () ->
                ok = maybe_insert_default_data(),
                
                ok = rabbit_exchange:recover(),
                ok = rabbit_amqqueue:recover(),
                ok = rabbit_realm:recover()
        end},
       {"persister",
        fun () -> ok = start_persister() end},
       {"builtin applications",
        fun () ->
                {ok, DefaultVHost} = application:get_env(default_vhost),
                ok = error_logger:add_report_handler(
                       rabbit_error_logger, [DefaultVHost]),
                ok = start_builtin_amq_applications()
        end},
       {"TCP listeners",
        fun () ->
                ok = rabbit_networking:start(),
                {ok, TCPListeners} = application:get_env(tcp_listeners),
                lists:foreach(
                  fun ({Host, Port}) ->
                          ok = rabbit_networking:start_tcp_listener(Host, Port)
                  end,
                  TCPListeners)
        end}]),

    io:format("~nbroker running~n"),

    {ok, SupPid}.

stop(_State) ->
    terminated_ok = error_logger:delete_report_handler(rabbit_error_logger),
    ok.

%---------------------------------------------------------------------------

print_banner() ->
    {ok, Product} = application:get_key(id),
    {ok, Version} = application:get_key(vsn),
    io:format("~s ~s (AMQP ~p-~p)~n~s~n~s~n~n",
              [Product, Version,
               ?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR,
               ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE]).

start_child(Mod) ->
    {ok,_} = supervisor:start_child(rabbit_sup,
                                    {Mod, {Mod, start_link, []},
                                     transient, 100, worker, [Mod]}),
    ok.

start_persister() ->
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_persister_sup,
                {rabbit_persister_sup, start_link, []},
                transient, infinity, supervisor,
                [rabbit_persister_sup]}),
    ok.

maybe_insert_default_data() ->
    case rabbit_mnesia:is_db_empty() of
        true -> insert_default_data();
        false -> ok
    end.

insert_default_data() ->
    {ok, DefaultUser} = application:get_env(default_user),
    {ok, DefaultPass} = application:get_env(default_pass),
    {ok, DefaultVHost} = application:get_env(default_vhost),
    ok = rabbit_access_control:add_vhost(DefaultVHost),
    ok = insert_default_user(DefaultUser, DefaultPass,
                             [{DefaultVHost, [<<"/data">>, <<"/admin">>]}]),
    ok.

insert_default_user(Username, Password, VHostSpecs) ->
    ok = rabbit_access_control:add_user(Username, Password),
    lists:foreach(
      fun ({VHostPath, Realms}) ->
              ok = rabbit_access_control:map_user_vhost(
                     Username, VHostPath),
              lists:foreach(
                fun (Realm) ->
                        RealmFullName = 
                            rabbit_misc:r(VHostPath, realm, Realm),
                        ok = rabbit_access_control:map_user_realm(
                               Username,
                               rabbit_access_control:full_ticket(
                                 RealmFullName))
                end, Realms)
      end, VHostSpecs),
    ok.

start_builtin_amq_applications() ->
    %%TODO: we may want to create a separate supervisor for these so
    %%they don't bring down the entire app when they die and fail to
    %%restart
    ok.

ensure_working_log_config() ->
    case error_logger:logfile(filename) of
        {error, no_log_file} ->
            %% either no log file was configured or opening it failed.
            case application:get_env(kernel, error_logger) of
                {ok, {file, Filename}} ->
                    case filelib:ensure_dir(Filename) of
                        ok -> ok;
                        {error, Reason1} ->
                            throw({error, {cannot_log_to_file,
                                           Filename, Reason1}})
                    end,
                    case error_logger:logfile({open, Filename}) of
                        ok -> ok;
                        {error, Reason2} ->
                            throw({error, {cannot_log_to_file,
                                           Filename, Reason2}})
                    end;
                _ -> ok
            end;
        _Filename -> ok
    end.
