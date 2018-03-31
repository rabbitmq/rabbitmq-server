<?php

require_once __DIR__.'/../vendor/autoload.php';

use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorage;
use Symfony\Component\Security\Core\User\InMemoryUserProvider;
use Symfony\Component\Security\Core\Authentication\AuthenticationProviderManager;
use Symfony\Component\Security\Core\Authorization\AccessDecisionManager;
use Symfony\Component\Security\Core\Authorization\AuthorizationChecker;
use RabbitMQAuth\Authentication\Authenticator;
use RabbitMQAuth\Authentication\ChainAuthenticationChecker;
use RabbitMQAuth\Authentication\UserPasswordTokenChecker;
use RabbitMQAuth\Authentication\UserTokenChecker;
use RabbitMQAuth\Authorization\DefaultVoter;
use RabbitMQAuth\Controller\AuthController;
use RabbitMQAuth\Security;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * You must can edit the following users and theyre roles (tags)
 */
$userProvider = new InMemoryUserProvider(array(
    //Admin user
    'Anthony' => array(
        'password' => 'anthony-password',
        'roles' => array(
            'administrator',
            // 'impersonator', // report to https://www.rabbitmq.com/validated-user-id.html
        ),
    ),
    'James' => array(
        'password' => 'bond',
        'roles' => array(
            'management',
        ),
    ),
    'Roger' => array(
        'password' => 'rabbit',
        'roles' => array(
            'monitoring',
        ),
    ),
    'Bunny' => array(
        'password' => 'bugs',
        'roles' => array(
            'policymaker',
        ),
    ),
));

/**
 * You can edit the user permissions here
 *
 * $permissions = arrray(
 *     '{USERNAME}' => array(
 *         '{VHOST}' => array(
 *             'ip' => '{REGEX_IP}',
 *             'read' => '{REGEX_READ}',
 *             'write' => '{REGEX_WRITE}',
 *             'configure' => '{REGEX_CONFIGURE}',
 *         ),
 *     ),
 * );
 */
$permissions = array(
    'Anthony' => array(
        'isAdmin' => true,
    ),
    'James' => array(
        '/' => array(
            'ip' => '.*',
            'read' => '.*',
            'write' => '.*',
            'configure' => '.*',
        ),
    ),
);

/**
 * Authenticator initialisation
 *
 * His gonna to find the user (with user provider) and to check the authentication with the authentication checker.
 *
 * We are 2 types of access token:
 *   - UserPasswordToken use with the user endpoint (to check the username and the password validity)
 *   - UserToken use with resource/topic/vhost endpoint (to check the username existence)
 */
$authenticator = new Authenticator(
    $userProvider,
    new ChainAuthenticationChecker(array(
        new UserPasswordTokenChecker(),
        new UserTokenChecker(),
    ))
);

/**
 * DefaultVoter is used to check the authorization.
 *
 * This class has the same implementation of default RabbitMQ authorization process.
 *
 * $permission is the configured user permission
 */
$defaultVoter = new DefaultVoter($permissions);

/**
 * This class is the initialisation of the symfony/security component
 */
$authenticationManager = new AuthenticationProviderManager(array($authenticator));
$accessDecisionManager = new AccessDecisionManager(array($defaultVoter));

$tokenStorage = new TokenStorage();

$authorizationChecker = new AuthorizationChecker(
    $tokenStorage,
    $authenticationManager,
    $accessDecisionManager
);

/**
 * The security class is the main class
 */
$security = new Security($authenticationManager, $authorizationChecker);

/**
 * This is the auth controller.
 *
 * It take the http request and return the http response
 */
$authController = new AuthController($tokenStorage, $security);

/** Add a logger */
$stream = new StreamHandler(__DIR__.'/../var/log.log', Logger::DEBUG);
$authenticator->setLogger((new Logger('rabbitmq_authenticator'))->pushHandler($stream));
$defaultVoter->setLogger((new Logger('rabbitmq_default_voter'))->pushHandler($stream));
$security->setLogger((new Logger('rabbitmq_security'))->pushHandler($stream));
