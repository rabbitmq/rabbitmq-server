-module(rabbit_cli_commands).

-include_lib("kernel/include/logger.hrl").

-export([argparse_def/0, run_command/5]).
-export([list_exchanges/5]).

argparse_def() ->
    %% Extract the commands from module attributes like feature flags and boot
    %% steps.
    #{commands =>
      #{"list" =>
       #{help => "List entities",
         commands =>
         #{"exchanges" =>
           maps:merge(
             rabbit_cli_io:argparse_def(record_stream),
             #{help => "List exchanges",
               handler => {?MODULE, list_exchanges}})
          }
        }
      }
     }.

run_command(Progname, ArgMap, CmdPath, Command, IO) ->
    %% TODO: Put both processes under the rabbit supervision tree.
    RunnerPid = command_runner(Progname, ArgMap, CmdPath, Command, IO),
    RunnerMRef = erlang:monitor(process, RunnerPid),
    receive
        {'DOWN', RunnerMRef, _, _, Reason} ->
            {ok, Reason}
    end.

command_runner(
  Progname, ArgMap, CmdPath, #{handler := {Mod, Fun}} = Command, IO) ->
    spawn_link(Mod, Fun, [Progname, ArgMap, CmdPath, Command, IO]).

list_exchanges(_Progname, ArgMap, _CmdPath, _Command, IO) ->
    InfoKeys = rabbit_exchange:info_keys(),
    Fields = lists:map(
               fun
                   (name = Key) ->
                       #{name => Key, type => resource};
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
                   (user_who_performed_action = Key) ->
                       #{name => Key, type => string};
                   (Key) ->
                       #{name => Key, type => term}
               end, InfoKeys),
    case rabbit_cli_io:start_record_stream(IO, exchanges, Fields, ArgMap) of
        {ok, Stream} ->
            Exchanges = rabbit_exchange:list(),
            lists:foreach(
              fun(Exchange) ->
                      Record0 = rabbit_exchange:info(Exchange),
                      Record1 = lists:sublist(Record0, length(Fields)),
                      Record2 = [Value || {_Key, Value} <- Record1],
                      rabbit_cli_io:push_new_record(IO, Stream, Record2)
              end, Exchanges),
            rabbit_cli_io:end_record_stream(IO, Stream);
        {error, _} = Error ->
            Error
    end.
