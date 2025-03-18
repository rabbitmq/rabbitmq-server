var lightStyles;
var darkStyles;
var darkSdhemeMedia;
var switcherRadios;

var initializeSwitcher = function initializeSwitcher() {
    lightStyles = document.querySelectorAll('link[rel=stylesheet][media*=prefers-color-scheme][media*=light]');
    darkStyles = document.querySelectorAll('link[rel=stylesheet][media*=prefers-color-scheme][media*=dark]');
    darkSdhemeMedia = matchMedia('(prefers-color-scheme: dark)');
    switcherRadios = document.getElementsByClassName('switcher__radio');

    let savedScheme = getSavedScheme();

    if (savedScheme !== null) {
        let currentRadio = document.querySelector(`.switcher__radio[value=${savedScheme}]`);
        if (currentRadio !== null) {
            currentRadio.checked = true;
        }
    }

    [...switcherRadios].forEach((radio) => {
        radio.addEventListener('change', (event) => {
            setScheme(event.target.value);
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