%define source_name rabbitmq-server

Name: rabbitmq-server
Version: %{rpm_version}
Release: 1
License: Mozilla Public License
Group: Development/Libraries
Source: http://www.rabbitmq.com/releases/%{source_name}-%{main_version}.tar.gz
URL: http://www.rabbitmq.com/
Vendor: LShift Ltd., Cohesive Financial Technologies LLC., Rabbit Technlogies Ltd.
%if 0%{?debian}
%else
BuildRequires: python, python-json
%endif
Requires: erlang
Packager: Hubert Plociniczak <hubert@lshift.net>
BuildRoot: %{_tmppath}/%{name}-%{main_version}-%{release}-root
Summary: The RabbitMQ server

%description
RabbitMQ is an implementation of AMQP, the emerging standard for high
performance enterprise messaging. The RabbitMQ server is a robust and
scalable implementation of an AMQP broker.

%define _libdir /usr/lib/erlang
%define _docdir /usr/share/doc
%define _mandir /usr/share/man
%define _maindir $RPM_BUILD_ROOT%{_libdir}/lib/rabbitmq_server-%{main_version}
%define package_name rabbitmq-server-dist

%pre
if [ $1 -gt 1 ]; then
  #Upgrade - stop and remove previous instance of rabbitmq init.d script
  /etc/init.d/rabbitmq-server stop
  /sbin/chkconfig --del rabbitmq-server
fi

%prep
%setup -n %{source_name}-%{main_version}

%build
mkdir %{package_name}
mkdir %{package_name}/sbin
mkdir %{package_name}/man
make install TARGET_DIR=`pwd`/%{package_name} \
             SBIN_DIR=`pwd`/%{package_name}/sbin \
             MAN_DIR=`pwd`/%{package_name}/man
             VERSION=%{main_version}

%install
mkdir -p %{_maindir}
mkdir -p $RPM_BUILD_ROOT%{_docdir}/rabbitmq-server
mkdir -p $RPM_BUILD_ROOT/etc/init.d
mkdir -p $RPM_BUILD_ROOT/usr/sbin
mkdir -p $RPM_BUILD_ROOT%{_mandir}

mkdir -p $RPM_BUILD_ROOT/var/lib/rabbitmq/mnesia
mkdir -p $RPM_BUILD_ROOT/var/log/rabbitmq

#Copy all necessary lib files etc.
cp -r %{package_name}/ebin %{_maindir}
cp -r %{package_name}/src %{_maindir}
cp -r %{package_name}/include %{_maindir}
chmod 755  %{package_name}/sbin/*
cp %{package_name}/sbin/* $RPM_BUILD_ROOT/usr/sbin/
cp -r %{package_name}/man/* $RPM_BUILD_ROOT%{_mandir}/

cp ../init.d $RPM_BUILD_ROOT/etc/init.d/rabbitmq-server
chmod 775 $RPM_BUILD_ROOT/etc/init.d/rabbitmq-server

mv $RPM_BUILD_ROOT/usr/sbin/rabbitmqctl $RPM_BUILD_ROOT/usr/sbin/rabbitmqctl_real
cp ../rabbitmqctl_wrapper $RPM_BUILD_ROOT/usr/sbin/rabbitmqctl
chmod 755 $RPM_BUILD_ROOT/usr/sbin/rabbitmqctl

cp %{buildroot}%{_mandir}/man1/rabbitmqctl.1.gz %{buildroot}%{_mandir}/man1/rabbitmqctl_real.1.gz

%post
# create rabbitmq group
if ! getent group rabbitmq >/dev/null; then
        groupadd -r rabbitmq
fi

# create rabbitmq user
if ! getent passwd rabbitmq >/dev/null; then
        useradd -r -g rabbitmq --home /var/lib/rabbitmq  rabbitmq
        usermod -c "Rabbit AMQP Messaging Server" rabbitmq
fi

# On 64bit /usr/lib64 contains Erlang, not /usr/lib. Fix with a symlink
ERL_LIB_DIR=$(erl -noshell -eval "io:format(\"~s~n\", [code:lib_dir()]), halt().")
if [ ! ${ERL_LIB_DIR} = "/usr/lib/erlang/lib" ] ; then 
        ln -s /usr/lib/erlang/lib/rabbitmq_server-%{main_version} ${ERL_LIB_DIR}
fi

chown -R rabbitmq:rabbitmq /var/lib/rabbitmq
chown -R rabbitmq:rabbitmq /var/log/rabbitmq

/sbin/chkconfig --add rabbitmq-server
/etc/init.d/rabbitmq-server start

%preun
if [ $1 = 0 ]; then
  #Complete uninstall
  /etc/init.d/rabbitmq-server stop
  /sbin/chkconfig --del rabbitmq-server

  # Remove symlink we added above
  ERL_LIB_DIR=$(erl -noshell -eval "io:format(\"~s~n\", [code:lib_dir()]), halt().")
  if [ ! ${ERL_LIB_DIR} = "/usr/lib/erlang/lib" ] ; then 
          rm ${ERL_LIB_DIR}/rabbitmq_server-%{main_version}
  fi
  
  # We do not remove log and lib directories
  # Leave rabbitmq user and group
fi

%files
%defattr(-,root,root)
%{_libdir}/lib/rabbitmq_server-%{main_version}/
%{_docdir}/rabbitmq-server/
%{_mandir}
/usr/sbin
/var/lib/rabbitmq
/var/log/rabbitmq
/etc/init.d/rabbitmq-server

%clean
rm -rf $RPM_BUILD_ROOT

%changelog
* Thu Jul 24 2008 Tony Garnock-Jones <tonyg@lshift.net> 1.4.0
- New upstream release

* Mon Mar 3 2008 Adrien Pierard <adrian@lshift.net> 1.3.0
- New upstream release

* Wed Sep 26 2007 Simon MacMullen <simon@lshift.net> 1.2.0
- New upstream release

* Wed Aug 29 2007 Simon MacMullen <simon@lshift.net> 1.1.1
- New upstream release

* Mon Jul 30 2007 Simon MacMullen <simon@lshift.net> 1.1.0-alpha
- New upstream release

* Tue Jun 12 2007 Hubert Plociniczak <hubert@lshift.net> hubert-20070607
- Building from source tarball, added starting script, stopping

* Mon May 21 2007 Hubert Plociniczak <hubert@lshift.net> 1.0.0-alpha
- Initial build of server library of RabbitMQ package
