%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc rabbitmq_aws supervisor for ETS table owner process
%% @end
%% ====================================================================
-module(rabbitmq_aws_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    % No children needed - just return empty supervisor
    _ = ets:new(aws_credentials, [named_table, public, {read_concurrency, true}]),
    _ = ets:new(aws_config, [named_table, public, {read_concurrency, true}]),
    {ok, {{one_for_one, 5, 10}, []}}.
