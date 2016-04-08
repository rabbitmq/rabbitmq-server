%%% ====================================================================
%%% @author Gavin M. Roy <gavinmroy@gmail.com>
%%% @copyright 2016, Gavin M. Roy
%%% @doc httpc_aws client library
%%% @end
%%% ====================================================================
-module(httpc_aws).

-behavior(gen_server).

%% API exports
-export([start_link/0,
         init/1,
         terminate/2,
         code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("httpc_aws.hrl").

%%====================================================================
%% gen_server functions
%%====================================================================

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init([term()]) ->
  {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore.
init([]) ->
  {ok, #state{}}.

terminate(_, _) ->
  ok.

code_change(_, _, State) ->
  {ok, State}.

handle_call(_Request, _From, State) ->
  {noreply, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

%%====================================================================
%% Internal functions
%%====================================================================
