package com.rabbitmq.authorization_server;

import java.util.List;
import java.util.stream.Stream;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import static com.rabbitmq.authorization_server.AudienceAuthority.aud;
import static com.rabbitmq.authorization_server.ScopeAuthority.scope;

@Component
@ConfigurationProperties(prefix = "spring.security.oauth2")
public class UsersConfiguration {

    private List<ConfigUser> users;
    
    public UsersConfiguration() {
    }

    @Override
    public String toString() {
        return "UsersConfiguration [users=" + users + "]";
    }

    public List<UserDetails> getUserDetails() {
        return users.stream().map(u -> 
             User.withDefaultPasswordEncoder()
				.username(u.getUsername())
				.password(u.getPassword())
				.authorities(u.getAuthorities())
				.build()).toList();
    }
    
    public static class ConfigUser {

        private String username;
        private String password;
        private List<String> scopes;
        private List<String> audiencies;
        
        public ConfigUser() {
        }
        
        public void setUsername(String username) {
            this.username = username;
        }
        public void setPassword(String password) {
            this.password = password;
        }
        public void setScopes(List<String> scopes) {
            this.scopes = scopes;
        }
        public void setAudiencies(List<String> audiencies) {
            this.audiencies = audiencies;
        }
        public String getUsername() {
            return username;
        }
        public String getPassword() {
            return password;
        }
        public List<String> getScopes() {
            return scopes;
        }
        public List<String> getAudiencies() {
            return audiencies;
        }
        public List<GrantedAuthority> getAuthorities() {
            return Stream.concat(scopes.stream().map(s -> scope(s)),
                audiencies.stream().map(s -> aud(s))).toList();
        }

        @Override
        public String toString() {
            return "User [username=" + username + ", password=" + password + ", scopes=" + scopes + ", audiencies="
                    + audiencies + "]";
        }

       
    }

    public List<ConfigUser> getUsers() {
        return users;
    }

    public void setUsers(List<ConfigUser> users) {
        this.users = users;
    }
}
