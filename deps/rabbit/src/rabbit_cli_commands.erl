-module(rabbit_cli_commands).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-export([argparse_def/0, run_command/1, do_run_command/1]).
-export([cmd_list_exchanges/1,
         cmd_import_definitions/1]).

-rabbitmq_command(
   {#{cli => ["declare", "exchange"],
      http => {put, ["exchanges", vhost, exchange]}},
    #{help => "Declare new exchange",
      arguments => [
                    #{name => vhost,
                      long => "-vhost",
                      type => binary,
                      default => <<"/">>,
                      help => "Name of the vhost owning the new exchange"},
                    #{name => exchange,
                      type => binary,
                      help => "Name of the exchange to declare"}
                   ],
      handler => {?MODULE, cmd_declare_exchange}}}).

-rabbitmq_command(
   {#{cli => ["list", "exchanges"],
      http => {get, ["exchanges"]}},
    [argparse_def_record_stream,
     #{help => "List exchanges",
       handler => {?MODULE, cmd_list_exchanges}}]}).

-rabbitmq_command(
   {#{cli => ["import", "definitions"]},
    [argparse_def_file_input,
     #{help => "Import definitions",
       handler => {?MODULE, cmd_import_definitions}}]}).

argparse_def() ->
    #{argparse_def := ArgparseDef} = get_discovered_commands(),
    ArgparseDef.

get_discovered_commands() ->
    Key = {?MODULE, discovered_commands},
    try
        persistent_term:get(Key)
    catch
        error:badarg ->
            Commands = discover_commands(),
            ArgparseDef = commands_to_cli_argparse_def(Commands),
            Cache = #{commands => Commands,
                      argparse_def => ArgparseDef},
            persistent_term:put(Key, Cache),
            Cache
    end.

discover_commands() ->
    %% Extract the commands from module attributes like feature flags and boot
    %% steps.
    ?LOG_DEBUG(
      "Commands: query commands in loaded applications",
      #{domain => ?RMQLOG_DOMAIN_CMD}),
    T0 = erlang:monotonic_time(),
    ScannedApps = rabbit_misc:rabbitmq_related_apps(),
    AttrsPerApp = rabbit_misc:module_attributes_from_apps(
                    rabbitmq_command, ScannedApps),
    T1 = erlang:monotonic_time(),
    ?LOG_DEBUG(
      "Commands: time to find supported commands: ~tp us",
      [erlang:convert_time_unit(T1 - T0, native, microsecond)],
      #{domain => ?RMQLOG_DOMAIN_CMD}),
    AttrsPerApp.

commands_to_cli_argparse_def(Commands) ->
    lists:foldl(
      fun({_App, _Mod, Entries}, Acc0) ->
              lists:foldl(
                fun
                    ({#{cli := Path}, Def}, Acc1) ->
                        Def1 = expand_argparse_def(Def),
                        M1 = lists:foldr(
                               fun
                                   (Cmd, undefined) ->
                                       #{commands => #{Cmd => Def1}};
                                   (Cmd, M0) ->
                                       #{commands => #{Cmd => M0}}
                               end, undefined, Path),
                        rabbit_cli:merge_argparse_def(Acc1, M1);
                    (_, Acc1) ->
                        Acc1
                end, Acc0, Entries)
      end, #{}, Commands).

expand_argparse_def(Def) when is_map(Def) ->
    Def;
expand_argparse_def(Defs) when is_list(Defs) ->
    lists:foldl(
      fun
          (argparse_def_record_stream, Acc) ->
              Def = rabbit_cli_io:argparse_def(record_stream),
              rabbit_cli:merge_argparse_def(Acc, Def);
          (argparse_def_file_input, Acc) ->
              Def = rabbit_cli_io:argparse_def(file_input),
              rabbit_cli:merge_argparse_def(Acc, Def);
          (Def, Acc) ->
              Def1 = expand_argparse_def(Def),
              rabbit_cli:merge_argparse_def(Acc, Def1)
      end, #{}, Defs).

run_command(Context) ->
    %% TODO: Put both processes under the rabbit supervision tree.
    Parent = self(),
    RunnerPid = spawn_link(
                  fun() ->
                          Ret = do_run_command(Context),
                          case Ret of
                              ok ->
                                  ok;
                              {ok, _} ->
                                  ok;
                              Other ->
                                  erlang:unlink(Parent),
                                  erlang:error(Other)
                          end
                  end),
    RunnerMRef = erlang:monitor(process, RunnerPid),
    receive
        {'DOWN', RunnerMRef, _, _, normal} ->
            ok;
        {'DOWN', RunnerMRef, _, _, Reason} ->
            Reason
    end.

do_run_command(#{command := #{handler := {Mod, Fun}}} = Context) ->
    erlang:apply(Mod, Fun, [Context]).

cmd_list_exchanges(#{arg_map := ArgMap, io := IO}) ->
    InfoKeys = rabbit_exchange:info_keys() -- [user_who_performed_action],
    Fields = lists:map(
               fun
                   (name = Key) ->
                       #{name => Key, type => string};
                   (type = Key) ->
                       #{name => Key, type => string};
                   (durable = Key) ->
                       #{name => Key, type => boolean};
                   (auto_delete = Key) ->
                       #{name => Key, type => boolean};
                   (internal = Key) ->
                       #{name => Key, type => boolean};
                   (arguments = Key) ->
                       #{name => Key, type => term};
                   (policy = Key) ->
                       #{name => Key, type => string};
                   (Key) ->
                       #{name => Key, type => term}
               end, InfoKeys),
    case rabbit_cli_io:start_record_stream(IO, exchanges, Fields, ArgMap) of
        {ok, Stream} ->
            Exchanges = rabbit_exchange:list(),
            lists:foreach(
              fun(Exchange) ->
                      Record0 = rabbit_exchange:info(Exchange, InfoKeys),
                      Record1 = lists:sublist(Record0, length(Fields)),
                      Record2 = [case Value of
                                     #resource{name = N} ->
                                         N;
                                     _ ->
                                         Value
                                 end || {_Key, Value} <- Record1],
                      rabbit_cli_io:push_new_record(IO, Stream, Record2)
              end, Exchanges),
            rabbit_cli_io:end_record_stream(IO, Stream),
            ok;
        {error, _} = Error ->
            Error
    end.

cmd_import_definitions(#{arg_map := ArgMap, io := IO}) ->
    case rabbit_cli_io:read_file(IO, ArgMap) of
        {ok, Data} ->
            rabbit_cli_io:format(IO, "Import definitions:~n  ~s~n", [Data]),
            ok;
        {error, _} = Error ->
            Error
    end.
