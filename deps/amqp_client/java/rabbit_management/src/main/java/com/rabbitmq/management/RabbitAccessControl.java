package com.rabbitmq.management;

import java.util.List;

/**
 * This is the Java interface for the rabbit_access_control module in Rabbit
 */
public interface RabbitAccessControl {

    void add_user(String name, String password);    

    void delete_user(String name);

    void change_password(String name, String password);

    List list_users();

    User lookup_user(String name);

    List list_vhosts();

    void add_vhost(String vhost);

    void delete_vhost(String vhost);

    List list_vhost_users(String vhost);

    List list_user_vhosts(String user);

    void map_user_vhost(String user, String vhost);

    void unmap_user_vhost(String user, String vhost);

    List list_user_realms(String user, String vhost);
    
    void map_user_realm(String user, Ticket ticket);

    /*
    action(set_permissions, Node,
       [Username, VHostPath, RealmName | Permissions]) ->
    io:format("Setting permissions for user ~p, vhost ~p, realm ~p ...",
              [Username, VHostPath, RealmName]),
    CheckedPermissions = check_permissions(Permissions),
    Ticket = #ticket{
      realm_name   = realm_rsrc(VHostPath, RealmName),
      passive_flag = lists:member(passive, CheckedPermissions),
      active_flag  = lists:member(active,  CheckedPermissions),
      write_flag   = lists:member(write,   CheckedPermissions),
      read_flag    = lists:member(read,    CheckedPermissions)},
    rpc_call(Node, rabbit_access_control, map_user_realm,
             [list_to_binary(Username), Ticket]);
             */
    
}