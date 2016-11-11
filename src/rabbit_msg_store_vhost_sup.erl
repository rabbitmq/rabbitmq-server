-module(rabbit_msg_store_vhost_sup).

-behaviour(supervisor2).

-export([start_link/3, init/1, add_vhost/2, delete_vhost/2,
         client_init/5, successfully_recovered_state/2]).

%% Internal
-export([start_vhost/4]).

start_link(Name, ClientRefs, StartupFunState) ->
    supervisor2:start_link({local, Name}, ?MODULE,
                           [Name, ClientRefs, StartupFunState]).

init([Name, ClientRefs, StartupFunState]) ->
    ets:new(Name, [named_table, public]),
    {ok, {{simple_one_for_one, 1, 1},
        [{rabbit_msg_store_vhost, {rabbit_msg_store_vhost_sup, start_vhost,
                                   [Name, ClientRefs, StartupFunState]},
           transient, infinity, supervisor, [rabbit_msg_store]}]}}.


add_vhost(Name, VHost) ->
    supervisor2:start_child(Name, [VHost]).

start_vhost(Name, ClientRefs, StartupFunState, VHost) ->
    case vhost_store_pid(Name, VHost) of
        no_pid ->
            VHostDir = vhost_store_dir(VHost),
            ok = rabbit_file:ensure_dir(VHostDir),
            case rabbit_msg_store:start_link(Name, VHostDir, ClientRefs, StartupFunState) of
                {ok, Pid} ->
                    ets:insert(Name, {VHost, Pid}),
                    {ok, Pid};
                Other     -> Other
            end;
        Pid when is_pid(Pid) ->
            {error, {already_started, Pid}}
    end.

delete_vhost(Name, VHost) ->
    case vhost_store_pid(Name, VHost) of
        no_pid               -> ok;
        Pid when is_pid(Pid) ->
            supervisor2:terminate_child(Name, Pid),
            cleanup_vhost_store(Name, VHost, Pid)
    end,
    ok.

client_init(Name, Ref, MsgOnDiskFun, CloseFDsFun, VHost) ->
    VHostPid = maybe_start_vhost(Name, VHost),
    rabbit_msg_store:client_init(VHostPid, Ref, MsgOnDiskFun, CloseFDsFun).

maybe_start_vhost(Name, VHost) ->
    case add_vhost(Name, VHost) of
        {ok, Pid}                       -> Pid;
        {error, {already_started, Pid}} -> Pid;
        Error                           -> throw(Error)
    end.

vhost_store_pid(Name, VHost) ->
    case ets:lookup(Name, VHost) of
        []    -> no_pid;
        [Pid] ->
            case erlang:is_process_alive(Pid) of
                true  -> Pid;
                false ->
                    cleanup_vhost_store(Name, VHost, Pid),
                    no_pid
            end
    end.

cleanup_vhost_store(Name, VHost, Pid) ->
    ets:delete_object(Name, {VHost, Pid}).

successfully_recovered_state(Name, VHost) ->
    case vhost_store_pid(Name, VHost) of
        no_pid               ->
            throw({message_store_not_started, Name, VHost});
        Pid when is_pid(Pid) ->
            rabbit_msg_store:successfully_recovered_state(Pid)
    end.

vhost_store_dir(VHost) ->
    Dir = rabbit_mnesia:dir(),
    EncodedName = list_to_binary(rabbit_vhost:vhost_name_to_dir_name(VHost)),
    binary_to_list(filename:join([Dir, EncodedName])).
