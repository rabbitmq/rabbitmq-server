from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    (r'^auth/user',     'auth.views.user'),
    (r'^auth/vhost',    'auth.views.vhost'),
    (r'^auth/resource', 'auth.views.resource'),
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),
    (r'^admin/', include(admin.site.urls)),
)
