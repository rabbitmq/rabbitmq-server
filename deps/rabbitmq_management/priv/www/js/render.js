// Rendering primitives with no assumption about which page, or which auth
// mechanism, is using them.
//
// Deliberately dependency-free of anything higher up the graph: main.js,
// oidc-oauth/helper.js, and auth-providers.js all import from here (the
// latter two specifically so login rendering can live next to the
// mechanism it belongs to instead of in main.js), so this file must not
// import back from any of them - only from true leaf modules (global.js,
// prefs.js) that have no imports of their own.

import { current_template, timer } from './global.js';
import { get_pref, store_pref, clear_pref, section_pref } from './prefs.js';

function replace_content(id, html) {
    $("#" + id).html(html);
}

function debug(str) {
    $('<p>' + str + '</p>').appendTo('#debug');
}

function format(template, json) {
    try {
        var fn = COMPILED_TEMPLATES[template];
        if (!fn) throw new Error('Template not found: ' + template);
        // Inject settings object
        json.settings = window.app_settings;
        return fn.call(json, json, json);
    } catch (err) {
        clearInterval(timer);
        console.log("Uncaught error: " + err);
        console.log("Stack: " + err['stack']);
        debug(err['name'] + ": " + err['message'] + "\n" + err['stack'] + "\n");
    }
}

function setup_visibility() {
    $('div.section,div.section-hidden').each(function(_index) {
        if ($(this).hasClass("disable-pref")) {
            return;
        }
        var pref = section_pref(current_template,
                                $(this).children('h2').text());
        var show = get_pref(pref);
        if (show == null) {
            show = $(this).hasClass('section');
        }
        else {
            show = show == 't';
        }
        if (show) {
            $(this).addClass('section-visible');
            // Workaround for... something. Although div.hider is
            // display:block anyway, not explicitly setting this
            // prevents the first slideToggle() from animating
            // successfully; instead the element just vanishes.
            $(this).find('.hider').attr('style', 'display:block;');
        }
        else {
            $(this).addClass('section-invisible');
        }
    });
}

function toggle_visibility(item) {
    var hider = item.next();
    var all = item.parent();
    var pref = section_pref(current_template, item.text());
    item.next().slideToggle(100);
    if (all.hasClass('section-visible')) {
        if (all.hasClass('section'))
            store_pref(pref, 'f');
        else
            clear_pref(pref);
        all.removeClass('section-visible');
        all.addClass('section-invisible');
    }
    else {
        if (all.hasClass('section-hidden')) {
            store_pref(pref, 't');
        } else {
            clear_pref(pref);
        }
        all.removeClass('section-invisible');
        all.addClass('section-visible');
    }
}

export { replace_content, format, setup_visibility, toggle_visibility, debug };

if (typeof window !== 'undefined') {
    Object.assign(window, { replace_content, format, setup_visibility, toggle_visibility, debug });
}
