dispatcher_add(function(sammy) {
        sammy.get('#/federation-links', function() {
                render({'links': '/federation-links'},
                       'federation-links', '#/federation-links');
            });
});

$("#tabs").append('<li class="administrator-only"><a href="#/federation-links">Federation</a></li>');

VHOST_QUERIES["/federation-links"] = "";
SORT_QUERIES["/federation-links"] = "";
