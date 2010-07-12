$(document).ready(function() {
    update();
});

function update() {
    with_req('/json/', function(text) {
            $(format(template_main(), JSON.parse(text))).appendTo("#main");
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
