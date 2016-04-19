%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_log).

-export([log/3, log/4]).
-export([debug/1, debug/2, debug/3,
         info/1, info/2, info/3,
         notice/1, notice/2, notice/3,
         warning/1, warning/2, warning/3,
         error/1, error/2, error/3,
         critical/1, critical/2, critical/3,
         alert/1, alert/2, alert/3,
         emergency/1, emergency/2, emergency/3,
         none/1, none/2, none/3]).

-include("rabbit_log.hrl").
%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(category() :: atom()).

-spec(log/3 :: (category(), lager:log_level(), string()) -> 'ok').
-spec(log/4 :: (category(), lager:log_level(), string(), [any()]) -> 'ok').

-spec(debug/1     :: (string()) -> 'ok').
-spec(debug/2     :: (string(), [any()]) -> 'ok').
-spec(debug/3     :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(info/1      :: (string()) -> 'ok').
-spec(info/2      :: (string(), [any()]) -> 'ok').
-spec(info/3      :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(notice/1    :: (string()) -> 'ok').
-spec(notice/2    :: (string(), [any()]) -> 'ok').
-spec(notice/3    :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(warning/1   :: (string()) -> 'ok').
-spec(warning/2   :: (string(), [any()]) -> 'ok').
-spec(warning/3   :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(error/1     :: (string()) -> 'ok').
-spec(error/2     :: (string(), [any()]) -> 'ok').
-spec(error/3     :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(critical/1  :: (string()) -> 'ok').
-spec(critical/2  :: (string(), [any()]) -> 'ok').
-spec(critical/3  :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(alert/1     :: (string()) -> 'ok').
-spec(alert/2     :: (string(), [any()]) -> 'ok').
-spec(alert/3     :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(emergency/1 :: (string()) -> 'ok').
-spec(emergency/2 :: (string(), [any()]) -> 'ok').
-spec(emergency/3 :: (pid() | [tuple()], string(), [any()]) -> 'ok').
-spec(none/1      :: (string()) -> 'ok').
-spec(none/2      :: (string(), [any()]) -> 'ok').
-spec(none/3      :: (pid() | [tuple()], string(), [any()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

log(Category, Level, Fmt) -> log(Category, Level, Fmt, []).

log(Category, Level, Fmt, Args) when is_list(Args) ->
    Sink = case Category of
        default -> ?LAGER_SINK;
        _       -> make_internal_sink_name(Category)
    end,
    lager:log(Sink, Level, self(), Fmt, Args).

make_internal_sink_name(Category) when Category == channel; 
                                       Category == connection; 
                                       Category == mirroring; 
                                       Category == queue;
                                       Category == federation ->
    lager_util:make_internal_sink_name(list_to_atom("rabbit_" ++ 
                                                    atom_to_list(Category)));
make_internal_sink_name(Category) -> 
    lager_util:make_internal_sink_name(Category).

debug(Format) -> debug(Format, []).
debug(Format, Args) -> debug(self(), Format, Args).
debug(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, debug, Metadata, Format, Args).

info(Format) -> info(Format, []).
info(Format, Args) -> info(self(), Format, Args).
info(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, info, Metadata, Format, Args).

notice(Format) -> notice(Format, []).
notice(Format, Args) -> notice(self(), Format, Args).
notice(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, notice, Metadata, Format, Args).

warning(Format) -> warning(Format, []).
warning(Format, Args) -> warning(self(), Format, Args).
warning(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, warning, Metadata, Format, Args).

error(Format) -> ?MODULE:error(Format, []).
error(Format, Args) -> ?MODULE:error(self(), Format, Args).
error(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, error, Metadata, Format, Args).

critical(Format) -> critical(Format, []).
critical(Format, Args) -> critical(self(), Format, Args).
critical(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, critical, Metadata, Format, Args).

alert(Format) -> alert(Format, []).
alert(Format, Args) -> alert(self(), Format, Args).
alert(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, alert, Metadata, Format, Args).

emergency(Format) -> emergency(Format, []).
emergency(Format, Args) -> emergency(self(), Format, Args).
emergency(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, emergency, Metadata, Format, Args).

none(Format) -> none(Format, []).
none(Format, Args) -> none(self(), Format, Args).
none(Metadata, Format, Args) ->
    lager:log(?LAGER_SINK, none, Metadata, Format, Args).
