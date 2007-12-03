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
    
    /*
    add_vhost/1
    delete_vhost/1
    list_vhosts/0,
    list_vhost_users/1
    list_user_vhosts/1
    map_user_vhost/2,
    unmap_user_vhost/2
    list_user_realms/2
    map_user_realm/2
    full_ticket/1
    */
}