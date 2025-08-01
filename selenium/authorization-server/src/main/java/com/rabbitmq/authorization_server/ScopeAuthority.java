package com.rabbitmq.authorization_server;

import java.util.List;
import java.util.Set;

import org.springframework.security.authentication.AbstractAuthenticationToken;
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

    @Override
    public String toString() {
        return "Scope:" + authority; 
    }

    public static List<String> getAuthorites(AbstractAuthenticationToken principal) {
        return principal.getAuthorities()
					.stream()
					.filter(a -> a instanceof ScopeAuthority)
					.map(a -> a.getAuthority()).toList();
    }
    
}