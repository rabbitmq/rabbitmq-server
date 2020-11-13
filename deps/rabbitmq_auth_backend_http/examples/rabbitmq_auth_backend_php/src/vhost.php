<?php

require_once '../vendor/autoload.php';
require_once 'bootstrap.php';

/**
 * The resource action handle the request and check the authentication + authorization of the request params
 * It check the QUERYSTRING params before the payload.
 */
$response = $authController->vhostAction(
    \Symfony\Component\HttpFoundation\Request::createFromGlobals() // Create an request object
);

$response->send(); // send the http response
