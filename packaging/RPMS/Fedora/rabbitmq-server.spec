%define debug_package %{nil}

Name: rabbitmq-server
Version: %%VERSION%%
Release: 1%{?dist}
License: MPLv1.1
Group: Development/Libraries
Source: http://www.rabbitmq.com/releases/rabbitmq-server/v%{version}/%{name}-%{version}.tar.gz
Source1: rabbitmq-server.init
Source2: rabbitmq-script-wrapper
Source3: rabbitmq-server.logrotate
Source4: rabbitmq-asroot-script-wrapper
URL: http://www.rabbitmq.com/
BuildArch: noarch
BuildRequires: erlang, python-simplejson
Requires: erlang, logrotate
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-%{_arch}-root
Summary: The RabbitMQ server
Requires(post): %%REQUIRES%%
Requires(pre): %%REQUIRES%%

%description
RabbitMQ is an implementation of AMQP, the emerging standard for high
performance enterprise messaging. The RabbitMQ server is a robust and
scalable implementation of an AMQP broker.

# We want to install into /usr/lib, even on 64-bit platforms
%define _rabbit_libdir %{_exec_prefix}/lib/rabbitmq
%define _rabbit_erllibdir %{_rabbit_libdir}/lib/rabbitmq_server-%{version}
%define _rabbit_wrapper %{_builddir}/`basename %{S:2}`
%define _rabbit_asroot_wrapper %{_builddir}/`basename %{S:4}`

%define _maindir %{buildroot}%{_rabbit_erllibdir}

%prep
%setup -q

%build
cp %{S:2} %{_rabbit_wrapper}
cp %{S:4} %{_rabbit_asroot_wrapper}
make %{?_smp_mflags}

%install
rm -rf %{buildroot}

make install TARGET_DIR=%{_maindir} \
             SBIN_DIR=%{buildroot}%{_rabbit_libdir}/bin \
             MAN_DIR=%{buildroot}%{_mandir}

mkdir -p %{buildroot}%{_localstatedir}/lib/rabbitmq/mnesia
mkdir -p %{buildroot}%{_localstatedir}/log/rabbitmq

#Copy all necessary lib files etc.
install -p -D -m 0755 %{S:1} %{buildroot}%{_initrddir}/rabbitmq-server
install -p -D -m 0755 %{_rabbit_wrapper} %{buildroot}%{_sbindir}/rabbitmqctl
install -p -D -m 0755 %{_rabbit_wrapper} %{buildroot}%{_sbindir}/rabbitmq-server
install -p -D -m 0755 %{_rabbit_wrapper} %{buildroot}%{_sbindir}/rabbitmq-multi
install -p -D -m 0755 %{_rabbit_asroot_wrapper} %{buildroot}%{_sbindir}/rabbitmq-activate-plugins
install -p -D -m 0755 %{_rabbit_asroot_wrapper} %{buildroot}%{_sbindir}/rabbitmq-deactivate-plugins

install -p -D -m 0644 %{S:3} %{buildroot}%{_sysconfdir}/logrotate.d/rabbitmq-server

mkdir -p %{buildroot}%{_sysconfdir}/rabbitmq

rm %{_maindir}/LICENSE %{_maindir}/LICENSE-MPL-RabbitMQ %{_maindir}/INSTALL

#Build the list of files
rm -f %{_builddir}/%{name}.files
echo '%defattr(-,root,root, -)' >> %{_builddir}/%{name}.files 
(cd %{buildroot}; \
    find . -type f ! -regex '\.%{_sysconfdir}.*' \
        ! -regex '\.\(%{_rabbit_erllibdir}\|%{_rabbit_libdir}\).*' \
        | sed -e 's/^\.//' >> %{_builddir}/%{name}.files)

%pre

if [ $1 -gt 1 ]; then
  # Upgrade - stop previous instance of rabbitmq-server init.d script
  /sbin/service rabbitmq-server stop
fi

# create rabbitmq group
if ! getent group rabbitmq >/dev/null; then
        groupadd -r rabbitmq
fi

# create rabbitmq user
if ! getent passwd rabbitmq >/dev/null; then
        useradd -r -g rabbitmq -d %{_localstatedir}/lib/rabbitmq rabbitmq \
            -c "RabbitMQ messaging server"
fi

%post
/sbin/chkconfig --add %{name}

%preun
if [ $1 = 0 ]; then
  #Complete uninstall
  /sbin/service rabbitmq-server stop
  /sbin/chkconfig --del rabbitmq-server
  
  # We do not remove /var/log and /var/lib directories
  # Leave rabbitmq user and group
fi

%files -f ../%{name}.files
%defattr(-,root,root,-)
%attr(0750, rabbitmq, rabbitmq) %dir %{_localstatedir}/lib/rabbitmq
%attr(0750, rabbitmq, rabbitmq) %dir %{_localstatedir}/log/rabbitmq
%dir %{_sysconfdir}/rabbitmq
%{_rabbit_erllibdir}
%{_rabbit_libdir}
%{_initrddir}/rabbitmq-server
%config(noreplace) %{_sysconfdir}/logrotate.d/rabbitmq-server
%doc LICENSE LICENSE-MPL-RabbitMQ

%clean
rm -rf %{buildroot}

%changelog
* Mon Feb 15 2010 Matthew Sackman <matthew@lshift.net> 1.7.2-1
- New Upstream Release

* Fri Jan 22 2010 Matthew Sackman <matthew@lshift.net> 1.7.1-1
- New Upstream Release

* Mon Oct 5 2009 David Wragg <dpw@lshift.net> 1.7.0-1
- New upstream release

* Wed Jun 17 2009 Matthias Radestock <matthias@lshift.net> 1.6.0-1
- New upstream release

* Tue May 19 2009 Matthias Radestock <matthias@lshift.net> 1.5.5-1
- Maintenance release for the 1.5.x series

* Mon Apr 6 2009 Matthias Radestock <matthias@lshift.net> 1.5.4-1
- Maintenance release for the 1.5.x series

* Tue Feb 24 2009 Tony Garnock-Jones <tonyg@lshift.net> 1.5.3-1
- Maintenance release for the 1.5.x series

* Mon Feb 23 2009 Tony Garnock-Jones <tonyg@lshift.net> 1.5.2-1
- Maintenance release for the 1.5.x series

* Mon Jan 19 2009 Ben Hood <0x6e6562@gmail.com> 1.5.1-1
- Maintenance release for the 1.5.x series

* Wed Dec 17 2008 Matthias Radestock <matthias@lshift.net> 1.5.0-1
- New upstream release

* Thu Jul 24 2008 Tony Garnock-Jones <tonyg@lshift.net> 1.4.0-1
- New upstream release

* Mon Mar 3 2008 Adrien Pierard <adrian@lshift.net> 1.3.0-1
- New upstream release

* Wed Sep 26 2007 Simon MacMullen <simon@lshift.net> 1.2.0-1
- New upstream release

* Wed Aug 29 2007 Simon MacMullen <simon@lshift.net> 1.1.1-1
- New upstream release

* Mon Jul 30 2007 Simon MacMullen <simon@lshift.net> 1.1.0-1.alpha
- New upstream release

* Tue Jun 12 2007 Hubert Plociniczak <hubert@lshift.net> 1.0.0-1.20070607
- Building from source tarball, added starting script, stopping

* Mon May 21 2007 Hubert Plociniczak <hubert@lshift.net> 1.0.0-1.alpha
- Initial build of server library of RabbitMQ package
