-module(rabbit_mqtt_clientid).

-behaviour(gen_server).

%% client API
-export([
         start_link/1,
         register/2,
         unregister/2
        ]).

%% debugging
-export([
         list_local/0,
         list_all/0,
         lookup_local/2
        ]).

%% gen_server callbacks
-export([
         init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2
        ]).

%% -------------------------------------------------------------------
%% client API
%% -------------------------------------------------------------------

start_link(ServerName) ->
    gen_server:start_link(ServerName, ?MODULE, #{}, []).

register(Vhost, ClientId)
  when is_binary(Vhost), is_binary(ClientId) ->
    gen_server:cast(?MODULE, {register, {Vhost, ClientId}, self()}).

unregister(Vhost, ClientId)
  when is_binary(Vhost), is_binary(ClientId) ->
    gen_server:cast(?MODULE, {unregister, {Vhost, ClientId}, self()}).

%% -------------------------------------------------------------------
%% debugging
%% -------------------------------------------------------------------

list_local() ->
    gen_server:call(?MODULE, list, 30_000).

list_all() ->
    {Replies, _BadNodes} = gen_server:multi_call([node() | nodes()], ?MODULE, list, 30_000),
    lists:flatmap(fun({_Node, Reply}) ->
                          Reply
                  end, Replies).

lookup_local(Vhost, ClientId)
  when is_binary(Vhost), is_binary(ClientId) ->
    gen_server:call(?MODULE, {lookup, {Vhost, ClientId}}, 30_000).

%% -------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init(#{}) ->
    Tid = ets:new(?MODULE, [private]),
    {ok, Tid}.

handle_cast({register, Key, Pid}, Tid) ->
    gen_server:abcast(nodes(), ?MODULE, {maybe_remove_duplicate, Key}),
    %% optimize for no duplicate ClientId
    case ets:insert_new(Tid, {Key, Pid}) of
        true ->
            ok;
        false ->
            remove_duplicate(Tid, Key),
            true = ets:insert(Tid, {Key, Pid})
    end,
    {noreply, Tid};

handle_cast({maybe_remove_duplicate, Key}, Tid) ->
    %% optimize for no duplicate ClientId
    case ets:member(Tid, Key) of
        false ->
            ok;
        true ->
            remove_duplicate(Tid, Key)
    end,
    {noreply, Tid};

handle_cast({unregister, Key, Pid}, Tid) ->
    true = ets:delete_object(Tid, {Key, Pid}),
    {noreply, Tid};

handle_cast(Request, Tid) ->
    rabbit_log:warning("~s received unkown request ~p", [?MODULE, Request]),
    {noreply, Tid}.

handle_call(list, _From, Tid) ->
    {reply, ets:tab2list(Tid), Tid};

handle_call({lookup, Key}, _From, Tid) ->
    {reply, ets:lookup(Tid, Key), Tid};

handle_call(Request, From, Tid) ->
    rabbit_log:warning("~s received unkown request ~p from ~p",
                       [?MODULE, Request, From]),
    {noreply, Tid}.

handle_info(Info, Tid) ->
    rabbit_log:warning("~s received unkown info ~p", [?MODULE, Info]),
    {noreply, Tid}.

terminate(Reason, Tid) ->
    rabbit_log:info("~s terminates with ~b entries and reason ~p",
                    [?MODULE, ets:info(Tid, size), Reason]).

%% -------------------------------------------------------------------
%% internal
%% -------------------------------------------------------------------

remove_duplicate(Tid, Key) ->
    [{Key, OldPid}] = ets:take(Tid, Key),
    gen_server:cast(OldPid, duplicate_id).
