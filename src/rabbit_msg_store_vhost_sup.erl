-module(rabbit_msg_store_vhost_sup).

-include("rabbit.hrl").

-behaviour(supervisor2).

-export([start_link/3, init/1, add_vhost/2, delete_vhost/2,
         client_init/5, successfully_recovered_state/2]).

%% Internal
-export([start_store_for_vhost/4]).

start_link(Type, VhostsClientRefs, StartupFunState) when is_map(VhostsClientRefs);
                                                         VhostsClientRefs == undefined ->
    supervisor2:start_link({local, Type}, ?MODULE,
                           [Type, VhostsClientRefs, StartupFunState]).

init([Type, VhostsClientRefs, StartupFunState]) ->
    ets:new(Type, [named_table, public]),
    {ok, {{simple_one_for_one, 1, 1},
        [{rabbit_msg_store_vhost, {rabbit_msg_store_vhost_sup, start_store_for_vhost,
                                   [Type, VhostsClientRefs, StartupFunState]},
           transient, infinity, supervisor, [rabbit_msg_store]}]}}.


add_vhost(Type, VHost) ->
    VHostPid = maybe_start_store_for_vhost(Type, VHost),
    {ok, VHostPid}.

start_store_for_vhost(Type, VhostsClientRefs, StartupFunState, VHost) ->
    case vhost_store_pid(Type, VHost) of
        no_pid ->
            VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
            ok = rabbit_file:ensure_dir(VHostDir),
            rabbit_log:info("Making sure message store directory '~s' for vhost '~s' exists~n", [VHostDir, VHost]),
            VhostRefs = refs_for_vhost(VHost, VhostsClientRefs),
            VhostStartupFunState = startup_fun_state_for_vhost(StartupFunState, VHost),
            case rabbit_msg_store:start_link(Type, VHostDir, VhostRefs, VhostStartupFunState) of
                {ok, Pid} ->
                    ets:insert(Type, {VHost, Pid}),
                    {ok, Pid};
                Other     -> Other
            end;
        Pid when is_pid(Pid) ->
            {error, {already_started, Pid}}
    end.

startup_fun_state_for_vhost({Fun, {start, [#resource{}|_] = QNames}}, VHost) ->
    QNamesForVhost = [QName || QName = #resource{virtual_host = VH} <- QNames,
                               VH == VHost ],
    {Fun, {start, QNamesForVhost}};
startup_fun_state_for_vhost(State, _VHost) -> State.

refs_for_vhost(_, undefined) -> undefined;
refs_for_vhost(VHost, Refs) ->
    case maps:find(VHost, Refs) of
        {ok, Val} -> Val;
        error -> []
    end.


delete_vhost(Type, VHost) ->
    case vhost_store_pid(Type, VHost) of
        no_pid               -> ok;
        Pid when is_pid(Pid) ->
            supervisor2:terminate_child(Type, Pid),
            cleanup_vhost_store(Type, VHost, Pid)
    end,
    ok.

client_init(Type, Ref, MsgOnDiskFun, CloseFDsFun, VHost) ->
    VHostPid = maybe_start_store_for_vhost(Type, VHost),
    rabbit_msg_store:client_init(VHostPid, Ref, MsgOnDiskFun, CloseFDsFun).

maybe_start_store_for_vhost(Type, VHost) ->
    case supervisor2:start_child(Type, [VHost]) of
        {ok, Pid}                       -> Pid;
        {error, {already_started, Pid}} -> Pid;
        Error                           -> throw(Error)
    end.

vhost_store_pid(Type, VHost) ->
    case ets:lookup(Type, VHost) of
        []    -> no_pid;
        [{VHost, Pid}] ->
            case erlang:is_process_alive(Pid) of
                true  -> Pid;
                false ->
                    cleanup_vhost_store(Type, VHost, Pid),
                    no_pid
            end
    end.

cleanup_vhost_store(Type, VHost, Pid) ->
    ets:delete_object(Type, {VHost, Pid}).

successfully_recovered_state(Type, VHost) ->
    case vhost_store_pid(Type, VHost) of
        no_pid               ->
            throw({message_store_not_started, Type, VHost});
        Pid when is_pid(Pid) ->
            rabbit_msg_store:successfully_recovered_state(Pid)
    end.
