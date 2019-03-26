%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/

%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(lager_forwarder_backend).

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2, terminate/2,
        code_change/3]).

-record(state, {
    next_sink :: atom(),
    level :: {'mask', integer()} | inherit
  }).

%% @private
init(Sink) when is_atom(Sink) ->
    init([Sink]);
init([Sink]) when is_atom(Sink) ->
    init([Sink, inherit]);
init([Sink, inherit]) when is_atom(Sink) ->
    {ok, #state{
        next_sink = Sink,
        level = inherit
      }};
init([Sink, Level]) when is_atom(Sink) ->
    try
        Mask = lager_util:config_to_mask(Level),
        {ok, #state{
            next_sink = Sink,
            level = Mask
          }}
    catch
        _:_ ->
            {error, {fatal, bad_log_level}}
    end;
init(_) ->
    {error, {fatal, bad_config}}.

%% @private
handle_call(get_loglevel, #state{next_sink = Sink, level = inherit} = State) ->
    SinkPid = whereis(Sink),
    Mask = case self() of
        SinkPid ->
            %% Avoid direct loops, defaults to 'info'.
            127;
        _ ->
            try
                Levels = [gen_event:call(SinkPid, Handler, get_loglevel,
                                         infinity)
                          || Handler <- gen_event:which_handlers(SinkPid)],
                lists:foldl(fun
                      ({mask, Mask}, Acc) ->
                          Mask bor Acc;
                      (Level, Acc) when is_integer(Level) ->
                          {mask, Mask} = lager_util:config_to_mask(
                            lager_util:num_to_level(Level)),
                          Mask bor Acc;
                      (_, Acc) ->
                          Acc
                  end, 0, Levels)
            catch
                exit:noproc ->
                    127
            end
    end,
    {ok, {mask, Mask}, State};
handle_call(get_loglevel, #state{level = Mask} = State) ->
    {ok, Mask, State};
handle_call({set_loglevel, inherit}, State) ->
    {ok, ok, State#state{level = inherit}};
handle_call({set_loglevel, Level}, State) ->
    try lager_util:config_to_mask(Level) of
        Mask ->
            {ok, ok, State#state{level = Mask}}
    catch
        _:_ ->
            {ok, {error, bad_log_level}, State}
    end;
handle_call(_Request, State) ->
    {ok, ok, State}.

%% @private
handle_event({log, LagerMsg}, #state{next_sink = Sink, level = Mask} = State) ->
    SinkPid = whereis(Sink),
    case self() of
        SinkPid ->
            %% Avoid direct loops.
            ok;
        _ ->
            case Mask =:= inherit orelse
                 lager_util:is_loggable(LagerMsg, Mask, ?MODULE) of
                true ->
                    case lager_config:get({Sink, async}, false) of
                        true  -> gen_event:notify(SinkPid, {log, LagerMsg});
                        false -> gen_event:sync_notify(SinkPid, {log, LagerMsg})
                    end;
                false ->
                    ok
            end
    end,
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

%% @private
handle_info(_Info, State) ->
    {ok, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
