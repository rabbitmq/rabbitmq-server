from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    (r'^auth/user',     'rabbitmq_auth_backend_django.auth.views.user'),
    (r'^auth/vhost',    'rabbitmq_auth_backend_django.auth.views.vhost'),
    (r'^auth/resource', 'rabbitmq_auth_backend_django.auth.views.resource'),
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),
    (r'^admin/', include(admin.site.urls)),
)
