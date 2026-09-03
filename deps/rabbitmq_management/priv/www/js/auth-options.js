// Which authentication options the login page should offer.
//
// Inputs (all from the oauth settings the server sends):
//   resource_servers          0..N oauth2 resources, already filtered for
//                             readiness by hasAnyResourceServerReady()
//   oauth_disable_basic_auth  whether basic auth is unavailable
//   strict_auth_mechanism     {type, resource_id} - allow ONLY this one
//   preferred_auth_mechanism  {type, resource_id} - preselect this one
//
// The two preference settings do different jobs, which the login template
// used to tangle together in overlapping booleans:
//   strict    FILTERS the option list down to one mechanism
//   preferred PRESELECTS one option; everything else stays collapsed
//
// Returns {mode, options, preselected}:
//   mode        'none'   nothing to log in with
//               'single' exactly one option
//               'choice' two or more; the page offers a choice
//   options     [{mechanism: 'oauth2'|'basic', id, label}]
//   preselected id of the explicitly preferred option, or null.
//               Note this reflects ONLY an explicit strict/preferred
//               setting: being the sole option does not preselect it,
//               because a lone section stays collapsed by default.

var BASIC_AUTH_OPTION_ID = 'basic';

function auth_mechanism_matches(mechanism, option) {
    if (mechanism == null || mechanism.type !== option.mechanism) return false;
    if (mechanism.type === 'oauth2' && mechanism.resource_id != null) {
        return mechanism.resource_id === option.id;
    }
    return true;
}

function authOptions(oauth) {
    var settings = oauth || {};
    var options = [];

    if (settings.enabled && Array.isArray(settings.resource_servers)) {
        for (var i = 0; i < settings.resource_servers.length; i++) {
            var resource = settings.resource_servers[i];
            options.push({
                mechanism: 'oauth2',
                id: resource.id,
                label: resource.label != null ? resource.label : resource.id
            });
        }
    }
    if (!settings.oauth_disable_basic_auth) {
        options.push({mechanism: 'basic', id: BASIC_AUTH_OPTION_ID, label: 'Basic Authentication'});
    }

    var strict = settings.strict_auth_mechanism;
    if (strict != null) {
        options = options.filter(function(option) {
            return auth_mechanism_matches(strict, option);
        });
    }

    var preselected = null;
    var preferred = settings.preferred_auth_mechanism;
    for (var j = 0; j < options.length; j++) {
        if (auth_mechanism_matches(preferred, options[j]) ||
            auth_mechanism_matches(strict, options[j])) {
            preselected = options[j].id;
            break;
        }
    }

    var mode = options.length === 0 ? 'none' : (options.length === 1 ? 'single' : 'choice');

    return {mode: mode, options: options, preselected: preselected};
}

function auth_options_for_mechanism(auth, mechanism) {
    return auth.options.filter(function(option) { return option.mechanism === mechanism; });
}

// The oauth2 section is expanded unless basic auth was explicitly
// preferred; basic auth is expanded only when it was. Exactly one of
// section-visible/section-invisible is emitted per section - never both,
// which the previous boolean soup could do.
function auth_section_is_expanded(auth, mechanism) {
    var basicPreselected = auth.preselected === BASIC_AUTH_OPTION_ID;
    return mechanism === 'basic' ? basicPreselected : !basicPreselected;
}

export {
    BASIC_AUTH_OPTION_ID,
    auth_mechanism_matches,
    authOptions,
    auth_options_for_mechanism,
    auth_section_is_expanded
};

if (typeof window !== 'undefined') {
    Object.assign(window, {
        BASIC_AUTH_OPTION_ID,
        auth_mechanism_matches,
        authOptions,
        auth_options_for_mechanism,
        auth_section_is_expanded
    });
}

