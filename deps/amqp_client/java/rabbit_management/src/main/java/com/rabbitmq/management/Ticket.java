package com.rabbitmq.management;

import java.io.Serializable;

public class Ticket implements Serializable {

    private Resource realm_name;
    
    private boolean passive_flag, active_flag, write_flag, read_flag;

    public Resource getRealm_name() {
        return realm_name;
    }

    public void setRealm_name(Resource realm_name) {
        this.realm_name = realm_name;
    }

    public boolean isPassive_flag() {
        return passive_flag;
    }

    public void setPassive_flag(boolean passive_flag) {
        this.passive_flag = passive_flag;
    }

    public boolean isActive_flag() {
        return active_flag;
    }

    public void setActive_flag(boolean active_flag) {
        this.active_flag = active_flag;
    }

    public boolean isWrite_flag() {
        return write_flag;
    }

    public void setWrite_flag(boolean write_flag) {
        this.write_flag = write_flag;
    }

    public boolean isRead_flag() {
        return read_flag;
    }

    public void setRead_flag(boolean read_flag) {
        this.read_flag = read_flag;
    }
}
