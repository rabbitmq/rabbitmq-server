$(document).ready(function() {
    const lightStyles = document.querySelectorAll('link[rel=stylesheet][media*=prefers-color-scheme][media*=light]');
    const darkStyles = document.querySelectorAll('link[rel=stylesheet][media*=prefers-color-scheme][media*=dark]');
    const darkSdhemeMedia = matchMedia('(prefers-color-scheme: dark)');
    const switcherRadios = document.getElementsByClassName('switcher__radio');

    function initializeSwitcher() {
        const savedScheme = getSavedScheme();

        if (savedScheme !== null) {
            const currentRadio = document.querySelector(`.switcher__radio[value=${savedScheme}]`);
            currentRadio.checked = true;
        }

        [...switcherRadios].forEach((radio) => {
            radio.addEventListener('change', (event) => {
                setScheme(event.target.value);
            });
        });
    }

    function initializeScheme() {
        const savedScheme = getSavedScheme();
        const systemScheme = getSystemScheme();

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
        const darkScheme = darkSdhemeMedia.matches;

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

    initializeSwitcher();
    initializeScheme();
});