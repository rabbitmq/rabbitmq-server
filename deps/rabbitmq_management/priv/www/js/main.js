$(document).ready(function() {
    update();
});

function update() {
    with_req('/json/', function(text) {
            $(format(template(), JSON.parse(text))).appendTo("#main");
    });
}

function format(template, json) {
    try {
        return jsontemplate.Template(template).expand(json);
    } catch (err) {
        alert(err['name'] + ": " + err['message']);
    }
}

function with_req(path, fun) {
    var json;
    var req = new XMLHttpRequest();
    req.open( "GET", path, true );
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            if (req.status == 200) {
                fun(req.responseText);
            }
            else {
                alert("Got response code " + req.status);
            }
        }
    };
    req.send(null);
}

function template() {
    return '<table class="warns"> \
 <tr><td><label>node = </label>{node}</td></tr> \
 <tr><td><label>pid = </label>{pid}</td></tr> \
 <tr><td><label>bound to = </label>{bound_to} </td></tr> \
 <tr class="{fd_warn}"><td><label>file descriptors (used/available)= </label>{fd_used} / {fd_total} </td></tr> \
 <tr class="{proc_warn}"><td><label>erlang processes (used/available) = </label>{proc_used} / {proc_total} </td></tr> \
 <tr class="{mem_warn}"><td><label>memory (used/available) = </label>{mem_used} / {mem_total} </td></tr> \
 <tr><td><label>ets memory = </label>{mem_ets} </td></tr> \
 <tr><td><label>binary memory = </label>{mem_binary}</td></tr> \
</table>';
}