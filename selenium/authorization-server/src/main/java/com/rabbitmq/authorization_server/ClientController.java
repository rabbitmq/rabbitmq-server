package com.rabbitmq.authorization_server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientController {

    @Autowired
    private RegisteredClientRepository registeredClientRepository;

    @GetMapping("/api/client")
    public RegisteredClient findClientById(@RequestParam String clientId) {
        return registeredClientRepository.findByClientId(clientId);
    }
}