package com.rabbitmq.authorization_server;

import org.springframework.security.core.GrantedAuthority;

public class AudienceAuthority implements GrantedAuthority {

    private String authority;
    
     
    public AudienceAuthority(String value) {
        this.authority = value;
    }

    public static AudienceAuthority aud(String value) {
        return new AudienceAuthority(value);
    }
    
    @Override
    public String getAuthority() {
        return authority;
    }
    
}