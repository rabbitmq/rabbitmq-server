-module(rabbit_amqp_sole_conn).

-include_lib("khepri/include/khepri.hrl").
-include("include/rabbit_khepri.hrl").

-export([acquire/4, release/2]).

-type vhost() :: binary().
-type container_id() :: binary().

-spec acquire(boolean(), vhost(), container_id(), pid()) ->
    ok | {error, refuse_connection}.
acquire(true, VHost, ContainerId, ConnectionPid) ->
    Path = khepri_sole_conn_path(VHost, ContainerId),
    case rabbit_khepri:adv_create(Path, ConnectionPid) of
        {ok, _} ->
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := _ExistingPid}}}} ->
            %% The container ID is already claimed by another connection
            {error, refuse_connection};
        {error, Reason} ->
            {error, Reason}
    end;
acquire(false, _, _, _) ->
    ok.

-spec release(vhost(), container_id()) -> ok.
release(VHost, ContainerId) ->
    Path = khepri_sole_conn_path(VHost, ContainerId),
    case rabbit_khepri:adv_delete(Path) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% --------------------------------------------------------------
%% Khepri paths
%% --------------------------------------------------------------

khepri_sole_conn_path(VHost, ContainerId)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(ContainerId) ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [amqp10_sole_conn, ContainerId]).
