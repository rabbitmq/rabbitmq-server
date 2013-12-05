dispatcher_add(function(sammy) {
        sammy.get('#/top', function() {
                render({'top': {path:    '/top',
                                options: {sort:true}}},
                       'top', '#/top');
            });
});

NAVIGATION['Admin'][0]['Top'] = ['#/top', 'administrator'];

function fmt_process_name_or_initial_call(process) {
    if (process == undefined) return '';

    if (process.registered_name != "") {
        return process.registered_name;
    }
    else {
        return process.initial_call;
    }
}
