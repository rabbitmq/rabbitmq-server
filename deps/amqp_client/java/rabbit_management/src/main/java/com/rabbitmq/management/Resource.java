package com.rabbitmq.management;

import java.io.Serializable;

public class Resource implements Serializable {

    private String virtual_host;

    public String getVirtual_host() {
        return virtual_host;
    }

    public void setVirtual_host(String virtual_host) {
        this.virtual_host = virtual_host;
    }
}
