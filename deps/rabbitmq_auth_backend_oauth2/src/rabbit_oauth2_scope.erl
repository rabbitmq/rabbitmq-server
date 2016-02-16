-module(rabbit_oauth2_scope).

-export([vhost_access/2, resource_access/3]).
-export([parse_scope/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%% API functions --------------------------------------------------------------

vhost_access(VHost, Ctx) ->
    lists:any(
      fun({#resource{ virtual_host = VH }, _}) ->
              VH == VHost
      end,
      get_scope_permissions(Ctx)).

resource_access(Resource, Permission, Ctx) ->
    lists:any(
      fun({Res, Perm}) ->
              Res == Resource andalso Perm == Permission
      end,
      get_scope_permissions(Ctx)).

%% Internal -------------------------------------------------------------------

get_scope_permissions(Ctx) -> 
    case lists:keyfind(<<"scope">>, 1, Ctx) of
        {_, Scope} -> 
            [ {Res, Perm} || {Res, Perm, _ScopeEl} <- parse_scope(Scope) ];
        false -> []
    end.

parse_scope(Scope) when is_list(Scope) ->
    lists:filtermap(
      fun(ScopeEl) ->
              case parse_scope_el(ScopeEl) of
                  ignore -> false;
                  Perm   -> {true, Perm}
              end
      end,
      Scope).

parse_scope_el(ScopeEl) when is_binary(ScopeEl) ->
    case binary:split(ScopeEl, <<"_">>, [global]) of
        [VHost, KindCode, PermCode | Name] ->
            Kind = case KindCode of
                       <<"q">>  -> queue;
                       <<"ex">> -> exchange;
                       <<"t">>  -> topic;
                       Other    -> binary_to_atom(Other, utf8)
                   end,
            Permission = case PermCode of
                             <<"configure">> -> configure;
                             <<"write">>     -> write;
                             <<"read">>      -> read;
                             _               -> ignore
                         end,
            case Kind == ignore orelse Permission == ignore orelse Name == [] of
                true  -> ignore;
                false ->
                    {
                  #resource{
                     virtual_host = VHost, 
                     kind = Kind, 
                     name = binary_join(Name, <<"_">>)},
                  Permission,
                  ScopeEl
                 }
            end;
        _ -> ignore
    end.

binary_join([B|Bs], Sep) ->
    iolist_to_binary([B|add_separator(Bs, Sep)]);                                                  
binary_join([], _Sep) ->                                       
    <<>>.                                                

add_separator([B|Bs], Sep) ->                               
    [Sep, B | add_separator(Bs, Sep)];                        
add_separator([], _) ->                                  
    [].                                                  


