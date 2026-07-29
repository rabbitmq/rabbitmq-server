// Feature flags page behaviour.
//
// This code lives in an external file, and the flag list reaches it through a
// data attribute rather than a serialised literal, so that the page works
// under a Content-Security-Policy without 'unsafe-inline'.

var nonreq_feature_flags = [];

// Called from postprocess_partial() after every render. The data element is
// inside the updatable section, so it carries fresh state on each refresh.
function feature_flags_refresh() {
    var data_element = document.getElementById('ff-feature-flags-data');
    if (!data_element) {
        nonreq_feature_flags = [];
        return;
    }
    nonreq_feature_flags = JSON.parse(data_element.dataset.featureFlags);

    var disabled_stable_feature_flags = false;
    for (var i = 0; i < nonreq_feature_flags.length; i++) {
        var feature_flag = nonreq_feature_flags[i];
        if (feature_flag.state == 'state_changing') {
            var checkbox = document.getElementById('ff-checkbox-' + feature_flag.name);
            if (checkbox) {
                /* The checkbox was already marked as disabled when the HTML was
                 * generated. */
                checkbox.indeterminate = true;
            }
        }
        if (feature_flag.state == 'disabled' && feature_flag.stability != 'experimental') {
            disabled_stable_feature_flags = true;
        }
    }

    if (disabled_stable_feature_flags) {
        var button = document.getElementById('ff-enable-all-button');
        button.disabled = false;

        var warning = document.getElementById('ff-disabled-stable-warning');
        warning.style.display = 'block';
    }
}

function enable_all_stable_feature_flags(button) {
    button.disabled = true;

    // The change handler is delegated on document, so the event has to bubble.
    const event = new Event('change', {bubbles: true});
    for (var i = 0; i < nonreq_feature_flags.length; i++) {
        var feature_flag = nonreq_feature_flags[i];
        if (feature_flag.stability == 'experimental' || feature_flag.state != 'disabled')
            continue;

        var checkbox = document.getElementById('ff-checkbox-' + feature_flag.name);
        if (checkbox.disabled)
            continue;

        checkbox.dispatchEvent(event);
    }
}

function lookup_feature_flag(feature_flag_name) {
    for (var i = 0; i < nonreq_feature_flags.length; i++) {
        var feature_flag = nonreq_feature_flags[i];
        if (feature_flag.name == feature_flag_name)
            return feature_flag;
    }
}

function handle_feature_flag(checkbox, feature_flag_name) {
    var feature_flag = lookup_feature_flag(feature_flag_name);

    pause_auto_refresh();

    checkbox.indeterminate = true;
    checkbox.disabled = true;
    checkbox.checked = false;

    if (feature_flag.stability == 'experimental') {
        var dialog = document.getElementById('ff-exp-dialog');
        var name_placeholders = document.querySelectorAll('#ff-exp-dialog .ff-name');
        var confirm_button = document.querySelector('#ff-exp-dialog #ff-exp-confirm');
        var cancel_button = document.querySelector('#ff-exp-dialog #ff-exp-cancel');
        var ack_supported = document.getElementById('ff-exp-ack-supported-checkbox');
        var ack_unsupported1 = document.getElementById('ff-exp-ack-unsupported-checkbox1');
        var ack_unsupported2 = document.getElementById('ff-exp-ack-unsupported-checkbox2');

        switch (feature_flag.experiment_level) {
            case 'unsupported':
                dialog.classList.remove('ff-exp-supported');
                dialog.classList.add('ff-exp-unsupported');
                break;
            case 'supported':
                dialog.classList.remove('ff-exp-unsupported');
                dialog.classList.add('ff-exp-supported');
                break;
        }

        dialog.returnValue = '';
        dialog.addEventListener('close', (event) => {
            if (dialog.returnValue == '') {
                checkbox.checked = false;
                checkbox.indeterminate = false;
                checkbox.disabled = false;

                resume_auto_refresh();
            }
        }, {once: true});

        /* Fill name placeholders with the feature flag name. */
        name_placeholders.forEach(function (name_placeholder) {
            name_placeholder.innerText = feature_flag.name;
        });

        var ack_listener = (event) => {
            switch (feature_flag.experiment_level) {
                case 'unsupported':
                    confirm_button.disabled = !(ack_unsupported1.checked && ack_unsupported2.checked);
                    break;
                case 'supported':
                    confirm_button.disabled = !ack_supported.checked;
                    break;
            }
            event.target.addEventListener('click', ack_listener, {once: true});
        };

        ack_supported.checked = false;
        ack_supported.addEventListener('click', ack_listener, {once: true});

        ack_unsupported1.checked = false;
        ack_unsupported1.addEventListener('click', ack_listener, {once: true});
        ack_unsupported2.checked = false;
        ack_unsupported2.addEventListener('click', ack_listener, {once: true});

        confirm_button.disabled = true;
        confirm_button.addEventListener('click', (event) => {
            event.stopPropagation();

            dialog.close('confirmed');
            enable_feature_flag(checkbox, feature_flag);
        }, {once: true});

        cancel_button.addEventListener('click', (event) => {
            dialog.close();
        }, {once: true});

        dialog.showModal();
    } else {
        enable_feature_flag(checkbox, feature_flag);
    }
}

function enable_feature_flag(checkbox, feature_flag) {
    var url = 'api/feature-flags/' + feature_flag.name + '/enable';
    var params = {name: feature_flag.name};

    feature_flag.state = 'state_changing';

    fetch(url, {
        method: 'PUT',
        headers: {
            'Authorization': authorization_header(),
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(params),
    }).then((resp) => {
        if (resp.ok) {
            feature_flag.state = 'enabled';
            checkbox.checked = true;
        } else {
            feature_flag.state = 'disabled';
            checkbox.checked = false;
            checkbox.disabled = false;

            resp.json().then((error) => {
                show_popup('warn', fmt_escape_html(error.reason));
            });
        }
        checkbox.indeterminate = false;

        return feature_flag;
    }).then((this_feature_flag) => {
        for (var i = 0; i < nonreq_feature_flags.length; i++) {
            var feature_flag = nonreq_feature_flags[i].name == this_feature_flag.name ?
                this_feature_flag : nonreq_feature_flags[i];

            if (feature_flag.stability != 'experimental' && feature_flag.state == 'disabled') {
                var button = document.getElementById('ff-enable-all-button');
                button.disabled = false;
                return;
            }
        }

        var warning = document.getElementById('ff-disabled-stable-warning');
        warning.style.display = 'none';
    }).then(() => {
        resume_auto_refresh();
    });
}
