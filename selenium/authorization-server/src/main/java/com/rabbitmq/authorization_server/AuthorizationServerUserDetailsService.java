package com.rabbitmq.authorization_server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import jakarta.annotation.PostConstruct;

public class AuthorizationServerUserDetailsService implements UserDetailsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationServerUserDetailsService.class);

    private final Map<String, User> usersByUsername = new HashMap<>();
    
    public AuthorizationServerUserDetailsService() {
        
    }
    UserDetails ud;

    @PostConstruct
    public void initUsers() {
        List<SimpleGrantedAuthority> roles = List.of("openid", "profile", "rabbitmq.tag:administrator").stream().map(scope ->
            new SimpleGrantedAuthority(scope)).toList();
        User rabbit_admin = new User("rabbit_admin", "rabbit_admin", roles);
        
        usersByUsername.put(rabbit_admin.getUsername(), rabbit_admin);

        LOGGER.info("Initialized users {}, {} and {}", rabbit_admin);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        if (usersByUsername.containsKey(username)) {
            LOGGER.info("Found user for {}", username);
            return usersByUsername.get(username);
        } else {   
            LOGGER.warn("No user found for {}", username);
            throw new UsernameNotFoundException("No user found for " + username);
        }
    }
}