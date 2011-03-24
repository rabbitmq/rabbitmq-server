#!/bin/sh
sudo apt-get install python-django
python manage.py syncdb
python manage.py runserver
