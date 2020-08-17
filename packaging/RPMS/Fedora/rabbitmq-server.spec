%define debug_package %{nil}
%define erlang_minver 21.3

Name: rabbitmq-server
Version: %%VERSION%%
Release: 1%{?dist}
License: MPLv1.1 and MIT and ASL 2.0 and BSD
Group: %{group_tag}
Source: http://www.rabbitmq.com/releases/rabbitmq-server/v%{upstream_version}/%{name}-%{upstream_version}.tar.xz
Source1: rabbitmq-server.init
Source2: rabbitmq-server.logrotate
Source3: rabbitmq-server.service
Source4: rabbitmq-server.tmpfiles
URL: http://www.rabbitmq.com/
BuildArch: noarch
%if 0%{?suse_version} >= 1315
BuildRequires: erlang-otp >= %{erlang_minver}
BuildRequires: erlang-otp-devel >= %{erlang_minver}
BuildRequires: elixir
BuildRequires: elixir-mix
%else
BuildRequires: erlang >= %{erlang_minver}
%endif
BuildRequires: gzip, sed, zip, rsync

%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
BuildRequires:  systemd
%endif

%if 0%{?suse_version} >= 1315
Requires: erlang >= %{erlang_minver}
Requires: erlang-crypto >= %{erlang_minver}
Requires: erlang-eldap >= %{erlang_minver}
Requires: erlang-hipe >= %{erlang_minver}
Requires: erlang-mnesia >= %{erlang_minver}
Requires: erlang-os_mon >= %{erlang_minver}
Requires: erlang-sasl >= %{erlang_minver}
Requires: erlang-ssl >= %{erlang_minver}
Requires: erlang-syntax_tools >= %{erlang_minver}
Requires: erlang-xmerl >= %{erlang_minver}
Requires: epmd >= %{erlang_minver}
%else
Requires: erlang >= %{erlang_minver}
%endif
Requires: logrotate, socat
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-%{_arch}-root
Summary: The RabbitMQ server

%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
Requires(pre): systemd
Requires(post): systemd
Requires(preun): systemd
%else
Requires(post): %%REQUIRES%%
Requires(pre): %%REQUIRES%%
%endif

%description
RabbitMQ is an open source multi-protocol messaging broker.

# We want to install into /usr/lib, even on 64-bit platforms
%define _rabbit_libdir %{_exec_prefix}/lib/rabbitmq
%define _rabbit_erllibdir %{_rabbit_libdir}/lib/rabbitmq_server-%{upstream_version}
%define _rabbit_server_ocf scripts/rabbitmq-server.ocf
%define _plugins_state_dir %{_localstatedir}/lib/rabbitmq/plugins
%define _rabbit_server_ha_ocf scripts/rabbitmq-server-ha.ocf
%define _rabbitmqctl_autocomplete scripts/rabbitmqctl-autocomplete.sh
%define _rabbitmq_user rabbitmq
%define _rabbitmq_group rabbitmq


%define _maindir %{buildroot}%{_rabbit_erllibdir}


%prep
%setup -q -n %{name}-%{upstream_version}

%build
cp -a deps/rabbit/docs/README-for-packages %{_builddir}/rabbitmq-server-%{upstream_version}/README
env -u DEPS_DIR make dist manpages

%install
rm -rf %{buildroot}

env -u DEPS_DIR make install install-bin install-man DESTDIR=%{buildroot} PREFIX=%{_exec_prefix} RMQ_ROOTDIR=%{_rabbit_libdir} RMQ_ERLAPP_DIR=%{_rabbit_erllibdir} MANDIR=%{_mandir}

mkdir -p %{buildroot}%{_localstatedir}/lib/rabbitmq/mnesia
mkdir -p %{buildroot}%{_localstatedir}/log/rabbitmq

#Copy all necessary lib files etc.

%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
install -p -D -m 0644 %{S:3} %{buildroot}%{_unitdir}/%{name}.service
%else
install -p -D -m 0755 %{S:1} %{buildroot}%{_initrddir}/rabbitmq-server
%endif

install -p -D -m 0755 %{_rabbit_server_ocf} %{buildroot}%{_exec_prefix}/lib/ocf/resource.d/rabbitmq/rabbitmq-server
install -p -D -m 0755 %{_rabbit_server_ha_ocf} %{buildroot}%{_exec_prefix}/lib/ocf/resource.d/rabbitmq/rabbitmq-server-ha
install -p -D -m 0644 %{S:2} %{buildroot}%{_sysconfdir}/logrotate.d/rabbitmq-server

install -p -D -m 0755 %{_rabbitmqctl_autocomplete} %{buildroot}%{_sysconfdir}/profile.d/rabbitmqctl-autocomplete.sh
install -p -D -m 0755 scripts/zsh_autocomplete.sh %{buildroot}%{_datarootdir}/zsh/vendor-functions/_enable_rabbitmqctl_completion


mkdir -p %{buildroot}%{_sysconfdir}/rabbitmq

mkdir -p %{buildroot}%{_sbindir}
sed -e 's|@STDOUT_STDERR_REDIRECTION@||' \
	-e 's|@RABBITMQ_USER@|%{_rabbitmq_user}|' -e 's|@RABBITMQ_GROUP@|%{_rabbitmq_group}|' \
	< scripts/rabbitmq-script-wrapper \
	> %{buildroot}%{_sbindir}/rabbitmqctl
chmod 0755 %{buildroot}%{_sbindir}/rabbitmqctl
for script in rabbitmq-server rabbitmq-plugins rabbitmq-diagnostics; do \
	cp -a %{buildroot}%{_sbindir}/rabbitmqctl \
	 %{buildroot}%{_sbindir}/$script; \
done

%if 0%{?fedora} > 14 || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
install -D -p -m 0644 %{SOURCE4} %{buildroot}%{_prefix}/lib/tmpfiles.d/%{name}.conf
%endif

rm %{_maindir}/LICENSE* %{_maindir}/INSTALL

#Build the list of files
echo '%defattr(-,root,root, -)' >%{_builddir}/%{name}.files
find %{buildroot} -path %{buildroot}%{_sysconfdir} -prune -o '!' -type d -printf "/%%P\n" >>%{_builddir}/%{name}.files
find %{buildroot} -path "*%{_initrddir}*" -type f -printf "/%%P\n" >>%{_builddir}/%{name}.files

%pre

# If the log directory exists, record its permissions so we can restore
# them after an upgrade. The goal is to set the permissions to 0750 on a
# fresh install but to keep permissions set by the user or a different
# default from a previous package.
if test -d /var/log/rabbitmq; then
    stat --format '%a' /var/log/rabbitmq > /var/log/rabbitmq/permissions
fi

if [ "$1" = 2 ]; then
  # Upgrade:
  # Stop previous instance of rabbitmq-server. But before doing this, we
  # also record if the service is running: it is used again in %post to
  # restart the service.
  if %{_sbindir}/rabbitmqctl status >/dev/null 2>&1; then
    touch %{_localstatedir}/lib/rabbitmq/rabbitmq-running-before-upgrade
  else
    rm -f %{_localstatedir}/lib/rabbitmq/rabbitmq-running-before-upgrade
  fi

%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
  systemctl stop rabbitmq-server
%else
  /sbin/service rabbitmq-server stop
%endif
fi

# create rabbitmq group
if ! getent group rabbitmq >/dev/null; then
        groupadd -r rabbitmq
fi

# create rabbitmq user
if ! getent passwd rabbitmq >/dev/null; then
        useradd -r -g rabbitmq -d %{_localstatedir}/lib/rabbitmq -s /sbin/nologin rabbitmq \
            -c "RabbitMQ messaging server"
fi

%post

%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
# %%systemd_post %%{name}.service
# manual expansion of systemd_post as this doesn't appear to
# expand correctly on debian machines
if [ $1 -eq 1 ] ; then
    # Initial installation
    systemctl preset %{name}.service >/dev/null 2>&1 || :
fi
systemctl daemon-reload
%else
/sbin/chkconfig --add %{name}
%endif

chmod -R o-rwx,g-w %{_localstatedir}/lib/rabbitmq/mnesia
chgrp rabbitmq %{_sysconfdir}/rabbitmq

# Restore permissions saved during %pre. See comment in %pre for the
# reason behind this.
if test -f /var/log/rabbitmq/permissions; then
    chmod "$(cat /var/log/rabbitmq/permissions)" /var/log/rabbitmq
    rm -f /var/log/rabbitmq/permissions
fi

if [ "$1" = 2 ] ; then
  # Upgrade:
  # Restart the service if it was running before the upgrade.
  if [ -f %{_localstatedir}/lib/rabbitmq/rabbitmq-running-before-upgrade ]; then
%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
    # %%systemd_postun_with_restart %%{name}.service
    # manual expansion of systemd_postun_with_restart as this doesn't appear to
    # expand correctly on debian machines
    systemctl restart %{name}.service >/dev/null 2>&1 || :
%else
    /sbin/service %{name} restart
%endif
    rm -f %{_localstatedir}/lib/rabbitmq/rabbitmq-running-before-upgrade
  fi
fi

if [ -n "$ZSH_VERSION" ]; then
    echo "Z Shell detected.
to enable rabbitmqctl autocompletion add the following to your .zshrc file:
autoload _enable_rabbitmqctl_completion; _enable_rabbitmqctl_completion"
fi

%preun

if [ $1 = 0 ]; then
  #Complete uninstall
%if 0%{?fedora} || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
  systemctl stop rabbitmq-server
%else
  /sbin/service rabbitmq-server stop
  /sbin/chkconfig --del rabbitmq-server
%endif

  # We do not remove /var/log and /var/lib directories
  # Leave rabbitmq user and group
fi

# Clean out plugin activation state, both on uninstall and upgrade
rm -rf %{_plugins_state_dir}
for ext in rel script boot ; do
    rm -f %{_rabbit_erllibdir}/ebin/rabbit.$ext
done

%postun

%if 0%{?fedora} > 17 || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1315
# For prior versions older than this, do a conversion
# from sysv to systemd
%triggerun -- %{name} < 3.6.5
# Save the current service runlevel info
# User must manually run systemd-sysv-convert --apply opensips
# to migrate them to systemd targets
systemd-sysv-convert --save %{name} >/dev/null 2>&1 ||:

# Run these because the SysV package being removed won't do them
/sbin/chkconfig --del %{name} >/dev/null 2>&1 || :
systemctl try-restart %{name}.service >/dev/null 2>&1 || :
%endif

%files -f ../%{name}.files
%defattr(-,root,root,-)
%attr(0755, rabbitmq, rabbitmq) %dir %{_localstatedir}/lib/rabbitmq
%attr(0750, rabbitmq, rabbitmq) %dir %{_localstatedir}/lib/rabbitmq/mnesia
%attr(0755, rabbitmq, rabbitmq) %dir %{_localstatedir}/log/rabbitmq
%attr(2755, -, rabbitmq) %dir %{_sysconfdir}/rabbitmq

%{_sysconfdir}/profile.d/rabbitmqctl-autocomplete.sh
%{_datarootdir}/zsh/vendor-functions/_enable_rabbitmqctl_completion

%config(noreplace) %{_sysconfdir}/logrotate.d/rabbitmq-server
%doc LICENSE*
%doc README
%doc deps/rabbit/docs/rabbitmq.config.example
%doc deps/rabbit/docs/set_rabbitmq_policy.sh.example

%clean
rm -rf %{buildroot}

%changelog
* Mon Aug 17 2020 info@rabbitmq.com 3.7.28-1
- New upstream release.

* Fri Jul 24 2020 info@rabbitmq.com 3.7.27-1
- New upstream release.

* Fri Jul 24 2020 info@rabbitmq.com 3.7.27~rc.2-1
- New upstream release.

* Wed Jun 24 2020 info@rabbitmq.com 3.7.27~rc.1-1
- New upstream release.

* Mon May 4 2020 info@rabbitmq.com 3.7.26-1
- New upstream release.

* Thu Apr 9 2020 info@rabbitmq.com 3.7.25-1
- New upstream release.

* Tue Apr 7 2020 info@rabbitmq.com 3.7.25~rc.1-1
- New upstream release.

* Wed Feb 12 2020 info@rabbitmq.com 3.7.24-1
- New upstream release.

* Mon Feb 10 2020 info@rabbitmq.com 3.7.24~rc.2-1
- New upstream release.

* Tue Feb 4 2020 info@rabbitmq.com 3.7.24~rc.1-1
- New upstream release.

* Sat Dec 21 2019 info@rabbitmq.com 3.7.24~beta.1-1
- New upstream release.

* Thu Dec 5 2019 info@rabbitmq.com 3.7.23-1
- New upstream release.

* Tue Dec 3 2019 info@rabbitmq.com 3.7.23~rc.1-1
- New upstream release.

* Wed Nov 27 2019 info@rabbitmq.com 3.7.22-1
- New upstream release.

* Thu Nov 21 2019 info@rabbitmq.com 3.7.22~rc.2-1
- New upstream release.

* Fri Nov 8 2019 info@rabbitmq.com 3.7.22~rc.1-1
- New upstream release.

* Fri Nov 1 2019 info@rabbitmq.com 3.7.21-1
- New upstream release.

* Fri Oct 25 2019 info@rabbitmq.com 3.7.20-1
- New upstream release.

* Thu Oct 24 2019 info@rabbitmq.com 3.7.20~rc.2-1
- New upstream release.

* Thu Oct 17 2019 info@rabbitmq.com 3.7.20~rc.1-1
- New upstream release.

* Thu Oct 10 2019 info@rabbitmq.com 3.7.20~beta.1-1
- New upstream release.

* Wed Oct 2 2019 info@rabbitmq.com 3.7.19-1
- New upstream release.

* Tue Sep 17 2019 info@rabbitmq.com 3.7.18-1
- New upstream release.

* Sat Sep 14 2019 info@rabbitmq.com 3.7.18~rc.1-1
- New upstream release.

* Thu Aug 22 2019 info@rabbitmq.com 3.7.18~beta.1-1
- New upstream release.

* Mon Jul 29 2019 info@rabbitmq.com 3.7.17-1
- New upstream release.

* Tue Jul 23 2019 info@rabbitmq.com 3.7.17~rc.3-1
- New upstream release.

* Sun Jul 21 2019 info@rabbitmq.com 3.7.17~rc.2-1
- New upstream release.

* Wed Jul 17 2019 info@rabbitmq.com 3.7.17~rc.1-1
- New upstream release.

* Tue Jul 9 2019 info@rabbitmq.com 3.7.17~beta.1-1
- New upstream release.

* Sun Jul 7 2019 info@rabbitmq.com 3.7.16-1
- New upstream release.

* Sun Jul 7 2019 info@rabbitmq.com 3.7.16~rc.4-1
- New upstream release.

* Wed Jun 19 2019 info@rabbitmq.com 3.7.16~beta.1-1
- New upstream release.

* Tue Jun 18 2019 info@rabbitmq.com 3.7.16~rc.3-1
- New upstream release.

* Sat May 18 2019 info@rabbitmq.com 3.7.15-1
- New upstream release.

* Wed May 8 2019 info@rabbitmq.com 3.7.15~beta.1-1
- New upstream release.

* Thu Mar 28 2019 info@rabbitmq.com 3.7.14-1
- New upstream release.

* Thu Mar 28 2019 info@rabbitmq.com 3.7.14~rc.2-1
- New upstream release.

* Fri Mar 22 2019 info@rabbitmq.com 3.7.14~rc.1-1
- New upstream release.

* Thu Mar 7 2019 info@rabbitmq.com 3.7.13-1
- New upstream release.

* Thu Feb 28 2019 info@rabbitmq.com 3.7.13~rc.2-1
- New upstream release.

* Wed Feb 27 2019 info@rabbitmq.com 3.7.13~rc.1-1
- New upstream release.

* Fri Feb 22 2019 info@rabbitmq.com 3.7.13~beta.1-1
- New upstream release.

* Fri Feb 15 2019 info@rabbitmq.com 3.7.12-1
- New upstream release.

* Sun Feb 10 2019 info@rabbitmq.com 3.7.12~rc.2-1
- New upstream release.

* Thu Feb 7 2019 info@rabbitmq.com 3.7.12~rc.1-1
- New upstream release.

* Thu Jan 31 2019 info@rabbitmq.com 3.7.11-1
- New upstream release.

* Fri Jan 25 2019 info@rabbitmq.com 3.7.11~rc.2-1
- New upstream release.

* Thu Jan 17 2019 info@rabbitmq.com 3.7.11~rc.1-1
- New upstream release.

* Mon Jan 7 2019 info@rabbitmq.com 3.7.10-1
- New upstream release.

* Thu Jan 3 2019 info@rabbitmq.com 3.7.10~rc.4-1
- New upstream release.

* Thu Dec 27 2018 info@rabbitmq.com 3.7.10~rc.3-1
- New upstream release.

* Wed Dec 19 2018 info@rabbitmq.com 3.7.10~rc.2-1
- New upstream release.

* Thu Dec 13 2018 info@rabbitmq.com 3.7.10~rc.1-1
- New upstream release.

* Fri Nov 16 2018 info@rabbitmq.com 3.7.9-1
- New upstream release.

* Tue Nov 6 2018 info@rabbitmq.com 3.7.9~rc.3-1
- New upstream release.

* Fri Oct 26 2018 info@rabbitmq.com 3.7.9~rc.2-1
- New upstream release.

* Thu Sep 20 2018 info@rabbitmq.com 3.7.8-1
- New upstream release.

* Mon Sep 17 2018 info@rabbitmq.com 3.7.8~rc.4-1
- New upstream release.

* Tue Aug 7 2018 info@rabbitmq.com 3.7.8~rc.3-1
- New upstream release.

* Tue Jul 31 2018 info@rabbitmq.com 3.7.8~rc.2-1
- New upstream release.

* Wed Jul 25 2018 info@rabbitmq.com 3.7.8~rc.1-1
- New upstream release.

* Thu Jul 5 2018 info@rabbitmq.com 3.7.7-1
- New upstream release.

* Thu Jun 28 2018 info@rabbitmq.com 3.7.7~rc.2-1
- New upstream release.

* Mon Jun 25 2018 info@rabbitmq.com 3.7.7~rc.1-1
- New upstream release.

* Tue Jun 19 2018 info@rabbitmq.com 3.7.7~beta.2-1
- New upstream release.

* Fri Jun 15 2018 info@rabbitmq.com 3.7.7~beta.1-1
- New upstream release.

* Wed Jun 13 2018 info@rabbitmq.com 3.7.6-1
- New upstream release.

* Tue Jun 5 2018 info@rabbitmq.com 3.7.6~rc.2-1
- New upstream release.

* Fri Jun 1 2018 info@rabbitmq.com 3.7.6~rc.1-1
- New upstream release.

* Thu May 10 2018 info@rabbitmq.com 3.7.5-1
- New upstream release.

* Wed May 2 2018 info@rabbitmq.com 3.7.5~rc.1-1
- New upstream release.

* Thu Apr 26 2018 info@rabbitmq.com 3.7.5~beta.3-1
- New upstream release.

* Wed Apr 4 2018 info@rabbitmq.com 3.7.5~beta.2-1
- New upstream release.

* Wed Mar 28 2018 info@rabbitmq.com 3.7.5~beta.1-1
- New upstream release.

* Wed Mar 7 2018 info@rabbitmq.com 3.7.4-1
- New upstream release.

* Tue Feb 27 2018 info@rabbitmq.com 3.7.4~rc.4-1
- New upstream release.

* Mon Feb 26 2018 info@rabbitmq.com 3.7.4~rc.3-1
- New upstream release.

* Fri Feb 23 2018 info@rabbitmq.com 3.7.4~rc.2-1
- New upstream release.

* Thu Feb 15 2018 info@rabbitmq.com 3.7.4~rc.1-1
- New upstream release.

* Tue Jan 30 2018 info@rabbitmq.com 3.7.3-1
- New upstream release.

* Thu Jan 25 2018 info@rabbitmq.com 3.7.3~rc.2-1
- New upstream release.

* Tue Jan 16 2018 info@rabbitmq.com 3.7.3~rc.1-1
- New upstream release.

* Sat Dec 23 2017 info@rabbitmq.com 3.7.2-1
- New upstream release.

* Thu Dec 21 2017 info@rabbitmq.com 3.7.1-1
- New upstream release.

* Wed Dec 13 2017 info@rabbitmq.com 3.7.1~beta.1-1
- New upstream release.

* Wed Nov 29 2017 info@rabbitmq.com 3.7.0-1
- New upstream release.

* Thu Nov 16 2017 info@rabbitmq.com 3.7.0~rc.2-1
- New upstream release.

* Mon Sep 18 2017 info@rabbitmq.com 3.7.0~rc.1-1
- New upstream release.

* Tue Sep 12 2017 info@rabbitmq.com 3.7.0~beta.20-1
- New upstream release.

* Mon Sep 11 2017 michael@rabbitmq.com 3.6.12-1
- New Upstream Release

* Wed Aug 16 2017 michael@rabbitmq.com 3.6.11-1
- New Upstream Release

* Thu May 25 2017 michael@rabbitmq.com 3.6.10-1
- New Upstream Release

* Wed Mar 29 2017 michael@rabbitmq.com 3.6.9-1
- New Upstream Release

* Fri Mar 17 2017 michael@rabbitmq.com 3.6.8-1
- New Upstream Release

* Wed Mar 15 2017 michael@rabbitmq.com 3.6.7-1
- New Upstream Release

* Mon Nov 21 2016 michael@rabbitmq.com 3.6.6-1
- New Upstream Release

* Fri Aug 5 2016 michael@rabbitmq.com 3.6.5-1
- New Upstream Release

* Fri Jul 29 2016 michael@rabbitmq.com 3.6.4-1
- New Upstream Release

* Wed Jul 6 2016 michael@rabbitmq.com 3.6.3-1
- New Upstream Release

* Thu May 19 2016 michael@rabbitmq.com 3.6.2-1
- New Upstream Release

* Tue Mar 1 2016 michael@rabbitmq.com 3.6.1-1
- New Upstream Release

* Tue Dec 22 2015 michael@rabbitmq.com 3.6.0-1
- New Upstream Release

* Tue Dec 15 2015 michael@rabbitmq.com 3.5.7-1
- New Upstream Release

* Wed Oct 7 2015 michael@rabbitmq.com 3.5.6-1
- New Upstream Release

* Thu Sep 24 2015 jean-sebastien@rabbitmq.com 3.5.5-3
- Fix bashism in rabbitmq-script-wrapper

* Thu Sep 24 2015 jean-sebastien@rabbitmq.com 3.5.5-1
- New Upstream Release

* Tue Jul 21 2015 michael@rabbitmq.com 3.5.4-1
- New Upstream Release

* Fri May 22 2015 jean-sebastien@rabbitmq.com 3.5.3-1
- New Upstream Release

* Tue May 12 2015 jean-sebastien@rabbitmq.com 3.5.2-1
- New Upstream Release

* Thu Apr 2 2015 michael@rabbitmq.com 3.5.1-1
- New Upstream Release

* Wed Mar 11 2015 jean-sebastien@rabbitmq.com 3.5.0-1
- New Upstream Release

* Wed Feb 11 2015 michael@rabbitmq.com 3.4.4-1
- New Upstream Release

* Tue Jan 6 2015 jean-sebastien@rabbitmq.com 3.4.3-1
- New Upstream Release

* Wed Nov 26 2014 simon@rabbitmq.com 3.4.2-1
- New Upstream Release

* Wed Oct 29 2014 simon@rabbitmq.com 3.4.1-1
- New Upstream Release

* Tue Oct 21 2014 simon@rabbitmq.com 3.4.0-1
- New Upstream Release

* Mon Aug 11 2014 simon@rabbitmq.com 3.3.5-1
- New Upstream Release

* Tue Jun 24 2014 simon@rabbitmq.com 3.3.4-1
- New Upstream Release

* Mon Jun 16 2014 simon@rabbitmq.com 3.3.3-1
- New Upstream Release

* Mon Jun 9 2014 simon@rabbitmq.com 3.3.2-1
- New Upstream Release

* Tue Apr 29 2014 simon@rabbitmq.com 3.3.1-1
- New Upstream Release

* Wed Apr 2 2014 simon@rabbitmq.com 3.3.0-1
- New Upstream Release

* Mon Mar 3 2014 simon@rabbitmq.com 3.2.4-1
- New Upstream Release

* Thu Jan 23 2014 emile@rabbitmq.com 3.2.3-1
- New Upstream Release

* Tue Dec 10 2013 emile@rabbitmq.com 3.2.2-1
- New Upstream Release

* Wed Oct 23 2013 emile@rabbitmq.com 3.2.0-1
- New Upstream Release

* Thu Aug 15 2013 simon@rabbitmq.com 3.1.5-1
- New Upstream Release

* Tue Jun 25 2013 tim@rabbitmq.com 3.1.3-1
- New Upstream Release

* Mon Jun 24 2013 tim@rabbitmq.com 3.1.2-1
- New Upstream Release

* Mon May 20 2013 tim@rabbitmq.com 3.1.1-1
- Test release

* Wed May 1 2013 simon@rabbitmq.com 3.1.0-1
- New Upstream Release

* Tue Dec 11 2012 simon@rabbitmq.com 3.0.1-1
- New Upstream Release

* Fri Nov 16 2012 simon@rabbitmq.com 3.0.0-1
- New Upstream Release

* Fri Dec 16 2011 steve@rabbitmq.com 2.7.1-1
- New Upstream Release

* Tue Nov 8 2011 steve@rabbitmq.com 2.7.0-1
- New Upstream Release

* Fri Sep 9 2011 tim@rabbitmq.com 2.6.1-1
- New Upstream Release

* Fri Aug 26 2011 tim@rabbitmq.com 2.6.0-1
- New Upstream Release

* Mon Jun 27 2011 simon@rabbitmq.com 2.5.1-1
- New Upstream Release

* Thu Jun 9 2011 jerryk@vmware.com 2.5.0-1
- New Upstream Release

* Thu Apr 7 2011 Alexandru Scvortov <alexandru@rabbitmq.com> 2.4.1-1
- New Upstream Release

* Tue Mar 22 2011 Alexandru Scvortov <alexandru@rabbitmq.com> 2.4.0-1
- New Upstream Release

* Thu Feb 3 2011 simon@rabbitmq.com 2.3.1-1
- New Upstream Release

* Tue Feb 1 2011 simon@rabbitmq.com 2.3.0-1
- New Upstream Release

* Mon Nov 29 2010 rob@rabbitmq.com 2.2.0-1
- New Upstream Release

* Tue Oct 19 2010 vlad@rabbitmq.com 2.1.1-1
- New Upstream Release

* Tue Sep 14 2010 marek@rabbitmq.com 2.1.0-1
- New Upstream Release

* Mon Aug 23 2010 mikeb@rabbitmq.com 2.0.0-1
- New Upstream Release

* Wed Jul 14 2010 Emile Joubert <emile@rabbitmq.com> 1.8.1-1
- New Upstream Release

* Tue Jun 15 2010 Matthew Sackman <matthew@rabbitmq.com> 1.8.0-1
- New Upstream Release

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
