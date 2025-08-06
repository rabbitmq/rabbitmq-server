package com.rabbitmq.authorization_server;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import java.util.List;

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
    
     @Override
    public String toString() {
        return "Audience:" + authority; 
    }

    public static List<String> getAll(AbstractAuthenticationToken principal) {
        return principal.getAuthorities()
					.stream().filter(a -> a instanceof AudienceAuthority)
					.map(a -> a.getAuthority()).toList();
    }
				
}