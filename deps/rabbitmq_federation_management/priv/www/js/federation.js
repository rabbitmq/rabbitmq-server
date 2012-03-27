dispatcher_add(function(sammy) {
    sammy.get('#/federation', function() {
            render({'links':       '/federation-links',
                    'connections': '/parameters/federation_connection'},
                'federation', '#/federation');
        });
    sammy.get('#/federation-connection/:id', function() {
            render({'connection': '/parameters/federation_connection/' + esc(this.params['id'])},
                'federation-connection', '#/federation');
        });
    sammy.put('#/fed-parameters', function() {
            var num_keys = ['expires', 'max_hops'];
            for (var i in num_keys) {
                num_key = num_keys[i];
                if (this.params[num_key] == '')
                    delete this.params[num_key];
                else
                    this.params[num_key] = parseInt(this.params[num_key]);
            }
            this.params = {"app_name": this.params.app_name,
                           "key":      this.params.key,
                           "value":    this.params};
            delete this.params.value.app_name;
            delete this.params.value.key;
            if (sync_put(this, '/parameters/:app_name/:key'))
                update();
            return false;
        });
    sammy.del('#/fed-parameters', function() {
            if (sync_delete(this, '/parameters/:app_name/:key'))
                go_to('#/federation');
            return false;
        });
});

$("#tabs").append('<li class="administrator-only"><a href="#/federation">Federation</a></li>');

VHOST_QUERIES["/federation-links"] = "";
SORT_QUERIES["/federation-links"] = "";

function link_fed_conn(name) {
    return _link_to(fmt_escape_html(name), '#/federation-connection/' + esc(name))
}
