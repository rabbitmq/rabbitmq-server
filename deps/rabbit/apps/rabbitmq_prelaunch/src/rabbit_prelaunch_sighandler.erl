-module(rabbit_prelaunch_sighandler).
-behaviour(gen_event).

-export([setup/0,
         init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% CAUTION: Signal handling in this module must be kept consistent
%% with the same handling in rabbitmq-server(8).

%% #{signal => default | ignore | stop}.
-define(SIGNALS_HANDLED_BY_US,
        #{
          %% SIGHUP is often used to reload the configuration or reopen
          %% log files after they were rotated. We don't support any
          %% of those two cases, so ignore it for now, until we can do
          %% something about it.
          sighup => ignore,

          %% SIGTSTP is triggered by Ctrl+Z to pause a program. However
          %% we can't handle SIGCONT, the signal used to resume the
          %% program. Unfortunately, it makes a SIGTSTP handler less
          %% useful here.
          sigtstp => ignore
         }).

-define(SIGNAL_HANDLED_BY_ERLANG(Signal),
        Signal =:= sigusr1 orelse
        Signal =:= sigquit orelse
        Signal =:= sigterm).

-define(SERVER, erl_signal_server).

setup() ->
    case os:type() of
        {unix, _} ->
            case whereis(?SERVER) of
                undefined ->
                    ok;
                _ ->
                    case lists:member(?MODULE, gen_event:which_handlers(?SERVER)) of
                        true  -> ok;
                        false -> gen_event:add_handler(?SERVER, ?MODULE, [])
                    end
            end;
        _ ->
            ok
    end.

init(_Args) ->
    maps:fold(
      fun
          (Signal, _, Ret) when ?SIGNAL_HANDLED_BY_ERLANG(Signal) -> Ret;
          (Signal, default, ok) -> os:set_signal(Signal, default);
          (Signal, ignore, ok)  -> os:set_signal(Signal, ignore);
          (Signal, _, ok)       -> os:set_signal(Signal, handle)
      end, ok, ?SIGNALS_HANDLED_BY_US),
    {ok, #{}}.

handle_event(Signal, State) when ?SIGNAL_HANDLED_BY_ERLANG(Signal) ->
    {ok, State};
handle_event(Signal, State) ->
    case ?SIGNALS_HANDLED_BY_US of
        %% The code below can be uncommented if we introduce a signal
        %% which should stop RabbitMQ.
        %
        %#{Signal := stop} ->
        %    error_logger:info_msg(
        %      "~s received - shutting down~n",
        %      [string:uppercase(atom_to_list(Signal))]),
        %    ok = init:stop();
        _ ->
            error_logger:info_msg(
              "~s received - unhandled signal~n",
              [string:uppercase(atom_to_list(Signal))])
    end,
    {ok, State}.

handle_info(_, State) ->
    {ok, State}.

handle_call(_, State) ->
    {ok, ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Args, _State) ->
    ok.
