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

    public static List<String> getAllUnauthorized(AbstractAuthenticationToken principal,
            Set<String> authorized) {
        return principal.getAuthorities()
					.stream()
					.filter(a -> a instanceof ScopeAuthority)
					.filter(a -> !authorized.contains(a.getAuthority()))
					.map(a -> a.getAuthority()).toList();
    }
    
}