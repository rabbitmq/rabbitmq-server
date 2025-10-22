-module(rabbit_queue_commands).

-export([add_member/5,
         list_with_local_promotable/1,
         delete_member/3,
         grow/5,
         peek/2,
         status/1,
         reclaim_memory/2,
         shrink_all/2]).

-include_lib("rabbit_common/include/resource.hrl").

%% TODO: membership is a subset of ra_membership() with 'unknown' removed.
-type membership() :: voter | non_voter | promotable.

-callback add_member(VHost :: rabbit_types:vhost(), Name :: resource_name(), Node :: node(), Membership :: membership(), Timeout :: timeout()) -> ok | {error, term()}.

-callback list_with_local_promotable() -> [amqqueue:amqqueue()] | {error, term()}.

-callback delete_member(VHost :: rabbit_types:vhost(), Name :: resource_name(), Node :: node()) -> ok | {error, term()}.

-callback peek(Pos :: non_neg_integer(), QName :: rabbit_amqqueue:name()) -> {ok, [{binary(), term()}]} | {error, term()}.

-callback status(QName :: rabbit_amqqueue:name()) -> [[{binary(), term()}]] | {error, term()}.

-callback reclaim_memory(QName :: rabbit_amqqueue:name()) ->  ok | {error, term()}.

-callback shrink_all(Node :: node()) -> [{rabbit_amqqueue:name(),
                                          {ok, pos_integer()} | {error, pos_integer(), term()}}] | {error, term()}.

-spec add_member(rabbit_types:vhost(), resource_name(), node(), membership(), timeout()) -> ok | {error, term()}.
add_member(VHost, Name, Node, Membership, Timeout) ->
    case rabbit_amqqueue:lookup(rabbit_misc:queue_resource(VHost, Name)) of
        {ok, Q} ->
            Type = amqqueue:get_type(Q),
            Type:add_member(VHost, Name, Node, Membership, Timeout);
        {error, not_found} = E ->
            E
    end.

-spec list_with_local_promotable(atom() | binary()) -> [amqqueue:amqqueue()] | {error, term()}.
list_with_local_promotable(TypeDescriptor) ->
    case rabbit_queue_type:lookup(TypeDescriptor) of
        {ok, Type} ->
            {ok, Type:list_with_local_promotable()};
        {error, not_found}  ->
            {error, {unknown_queue_type, TypeDescriptor}}
    end.

-spec delete_member(rabbit_types:vhost(), resource_name(), node()) -> ok | {error, term()}.
delete_member(VHost, Name, Node) ->
    case rabbit_amqqueue:lookup(rabbit_misc:queue_resource(VHost, Name)) of
        {ok, Q} ->
            Type = amqqueue:get_type(Q),
            Type:delete_member(VHost, Name, Node);
        {error, not_found} = E ->
            E
    end.

-spec grow(node(), binary(), binary(), all | even, membership()) ->
          [{rabbit_amqqueue:name(),
            {ok, pos_integer()} | {error, pos_integer(), term()}}].
grow(Node, VhostSpec, QueueSpec, Strategy, Membership) ->
    Running = rabbit_nodes:list_running(),
    [begin
         Size = length(amqqueue:get_nodes(Q)),
         QName = amqqueue:get_name(Q),
         rabbit_log:info("~ts: adding a new member (replica) on node ~w",
                         [rabbit_misc:rs(QName), Node]),
         Type = amqqueue:get_type(Q),
         case Type:add_member(Q, Node, Membership) of
             ok ->
                 {QName, {ok, Size + 1}};
             {error, Err} ->
                 rabbit_log:warning(
                   "~ts: failed to add member (replica) on node ~w, error: ~w",
                   [rabbit_misc:rs(QName), Node, Err]),
                 {QName, {error, Size, Err}}
         end
     end
     || Q <- rabbit_amqqueue:list(),
        rabbit_queue_type:is_replicable(Q),
        %% don't add a member if there is already one on the node
        not lists:member(Node, amqqueue:get_nodes(Q)),
        %% node needs to be running
        lists:member(Node, Running),
        matches_strategy(Strategy, amqqueue:get_nodes(Q)),
        is_match(amqqueue:get_vhost(Q), VhostSpec) andalso
        is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec) ].

-spec peek(Pos :: non_neg_integer(), QName :: rabbit_amqqueue:name()) -> {ok, [{binary(), term()}]} | {error, term()}.
peek(Pos, QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Type = amqqueue:get_type(Q),
            Type:peek(Pos, QName);
        {error, not_found} = E ->
            E
    end.

-spec status(QName :: rabbit_amqqueue:name()) -> [[{binary(), term()}]] | {error, term()}.
status(QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Type = amqqueue:get_type(Q),
            Type:status(QName);
        {error, not_found} = E ->
            E
    end.

-spec reclaim_memory(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) -> ok | {error, term()}.
reclaim_memory(VHost, QueueName) ->
    QName = #resource{virtual_host = VHost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Type = amqqueue:get_type(Q),
            Type:reclaim_memory(QName);
        {error, not_found} = E ->
            E
    end.

-spec shrink_all(Node :: node(), TypeDescriptor :: rabbit_registry:type_descriptor()) ->
    {ok, pos_integer()} |
    {error, pos_integer(), term()} |
    {error, {unknown_queue_type, rabbit_registry:type_descriptor()}} |
    {error, atom()}. %% to cover not_supported
shrink_all(Node, <<"all">>) ->
    lists:flatten([Type:shrink_all(Node) || Type <- rabbit_queue_type:list_replicable()]);
shrink_all(Node, TypeDescriptor) ->
    case rabbit_queue_type:lookup(TypeDescriptor) of
        {ok, Type} ->
            {ok, Type:shrink_all(Node)};
        {error, not_found}  ->
            {error, {unknown_queue_type, TypeDescriptor}}
    end.

matches_strategy(all, _) -> true;
matches_strategy(even, Members) ->
    length(Members) rem 2 == 0.

is_match(Subj, E) ->
   nomatch /= re:run(Subj, E).

get_resource_name(#resource{name  = Name}) ->
    Name.
