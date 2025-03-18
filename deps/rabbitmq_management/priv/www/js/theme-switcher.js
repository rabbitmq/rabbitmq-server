var lightStyles;
var darkStyles;
var darkSdhemeMedia;

function initializeSwitcher() {
    lightStyles = document.querySelectorAll('link[rel=stylesheet][media*=prefers-color-scheme][media*=light]');
    darkStyles = document.querySelectorAll('link[rel=stylesheet][media*=prefers-color-scheme][media*=dark]');
    darkSdhemeMedia = matchMedia('(prefers-color-scheme: dark)');

    let savedScheme = getSavedScheme();
    let switcherButtons = document.getElementsByClassName('theme-switcher');

    if(switcherButtons.length === 0) return;
    
    if(savedScheme !== null)
    {
        switcherButtons[0].setAttribute("x-scheme", savedScheme);
    }

    [...switcherButtons].forEach((button) => {
        button.addEventListener('click', function() {
            let currentScheme = switcherButtons[0].getAttribute("x-scheme");
            let systemScheme = getSystemScheme();
            let newScheme;
            switch (currentScheme) {
                case "dark":
                    if(systemScheme === "dark")
                    {
                        newScheme = "auto";
                    }
                    else
                    {
                        newScheme = "light";
                    }
                    break;
                case "light":
                    if(systemScheme === "light")
                    {
                        newScheme = "auto";
                    }
                    else
                    {
                        newScheme = "dark";
                    }
                    break;
                default:
                    if(systemScheme === "light")
                    {
                        newScheme = "dark";
                    }
                    else
                    {
                        newScheme = "light";
                    }
                    break;
            }

            setScheme(newScheme);
            button.setAttribute("x-scheme", newScheme);
            button.setAttribute("title", `Switch between dark and light mode (currently ${newScheme} mode)`);
            button.setAttribute("aria-label", `Switch between dark and light mode (currently ${newScheme} mode)`);
        });
    });
}

var initializeScheme = function initializeScheme() {
    let savedScheme = getSavedScheme();
    let systemScheme = getSystemScheme();

    if (savedScheme == null) return;

    if(savedScheme !== systemScheme) {
        setScheme(savedScheme);
    }
}

function setScheme(scheme) {
    switchMediaScheme(scheme);

    if (scheme === 'auto') {
        clearScheme();
    } else {
        saveScheme(scheme);
    }
}

function switchMediaScheme(scheme) {
    let lightMedia;
    let darkMedia;

    if (scheme === 'auto') {
        lightMedia = '(prefers-color-scheme: light)';
        darkMedia = '(prefers-color-scheme: dark)';
    } else {
        lightMedia = (scheme === 'light') ? 'all' : 'bot all';
        darkMedia = (scheme === 'dark') ? 'all' : 'bot all';
    }

    [...lightStyles].forEach((link) => {
        link.media = lightMedia;
    });

    [...darkStyles].forEach((link) => {
        link.media = darkMedia;
    });
}

function getSystemScheme() {
    let darkScheme = darkSdhemeMedia.matches;

    return darkScheme ? 'dark' : 'light';
}

function getSavedScheme() {
    return localStorage.getItem('color-scheme');
}

function saveScheme(scheme) {
    localStorage.setItem('color-scheme', scheme);
}

function clearScheme() {
    localStorage.removeItem('color-scheme');
}

$(window).on('popstate', function() {
    initializeSwitcher();
    initializeScheme();
});

$(document).ready(function() {
    initializeSwitcher();
    initializeScheme();
});
