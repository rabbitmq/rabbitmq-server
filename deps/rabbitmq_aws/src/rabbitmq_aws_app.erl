%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc rabbitmq_aws application startup
%% @end
%% ====================================================================
-module(rabbitmq_aws_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case rabbitmq_aws_sup:start_link() of
        {ok, Pid} ->
            rabbitmq_aws_resource_fetcher:process_arns(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.
