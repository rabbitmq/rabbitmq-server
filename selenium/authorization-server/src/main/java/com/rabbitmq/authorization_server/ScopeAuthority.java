package com.rabbitmq.authorization_server;

import org.springframework.security.core.GrantedAuthority;

public class ScopeAuthority implements GrantedAuthority {

    private String authority;
    
    public ScopeAuthority(String value) {
        this.authority = value;
    }

    public static ScopeAuthority scope(String value) {
        return new ScopeAuthority(value);
    }
    
    @Override
    public String getAuthority() {
        return authority;
    }
    
}