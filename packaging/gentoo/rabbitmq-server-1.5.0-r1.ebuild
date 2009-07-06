# copyright 1999-2008 gentoo foundation
# distributed under the terms of the gnu general public license v2
# $header: $

inherit eutils
DESCRIPTION="RabbitMQ is a high-performance AMQP-compliant message broker written in Erlang."
HOMEPAGE="http://www.rabbitmq.com/" 
SRC_URI="http://www.rabbitmq.com/releases/${PN}/v${PV}/${P}.tar.gz"
LICENSE="MPL"
SLOT="0" 
KEYWORDS="~alpha amd64 ~ppc ~ppc64 ~sparc x86"
IUSE="+docs"

# runtime time deps
RDEPEND="dev-lang/erlang
		 app-admin/logrotate"

# build time deps
DEPEND="dev-lang/erlang
		dev-python/simplejson"

src_install()
{
# Erlang module
	einfo "Installing rabbit erlang module"
	local targetdir="/usr/$(get_libdir)/erlang/lib/${P}"
	dodir "${targetdir}"		\
		|| die "failed to create ${targetdir} for ${P}"

	cp -dpr ${S}/ebin ${S}/include "${D}/${targetdir}" \
		|| die "failed to install erlang module for ${P}"
	
	fperms  700 ${targetdir} \
		|| die "failed to chmod erlang module for ${P}"
	
	fowners rabbitmq:rabbitmq ${targetdir} \
		|| die "failed to chown erlang module for ${P}"

# Server scripts
	einfo "Installing rabbit scripts"
	cd  ${S}/scripts
	dosbin ${PN/server/multi}	\
		|| die "failed to install rabbitmq-multi for ${P}"
	dosbin ${PN}				\
		|| die "failed to install rabbitmq-server for ${P}"
	dosbin ${PN/-server/ctl}	\
		|| die "failed to install rabbitmqctl for ${P}"
	dosbin ${FILESDIR}/${PV}/misc/${PN/server/invoke}	\
		|| die "failed to install rabbitmq-invoke for ${P}"

# Docs
	if use docs; then
		einfo "Installing rabbit docs"
		cd ${S}
		dodoc INSTALL LICENSE LICENSE-MPL-RabbitMQ \
			|| die "Failed when installing rabbit docs"
	fi

# Man pages	
	einfo "installing rabbit man pages"
	doman ${FILESDIR}/${PV}/man/${PN/server/multi.1}	\
		|| die "Install of rabbitmq-multi manpage failed"

	doman ${FILESDIR}/${PV}/man/${PN/server/server.1} \
		|| die "Install of rabbitmq-server manpage failed"

	doman ${FILESDIR}/${PV}/man/${PN/-server/.5}		\
		|| die "Install of rabbitmq manpage failed"

	doman ${FILESDIR}/${PV}/man/${PN/-server/ctl.1}	\
		|| die "Install of rabbitmqctl manpage failed"

# Server configuration
	einfo "Installing rabbit configuration"
	local fname=${PN/server/cluster.example} 
	newconfd ${FILESDIR}/${PV}/init.d/${PN}.confd ${PN}		\
		|| die "failed to install conf.d file for ${P}"

# Example clustering configuration
	einfo "Installing example rabbit cluster configuration"
	newconfd ${FILESDIR}/${PV}/init.d/${fname}.confd ${fname} \
		|| die "failed to install ${fname} for ${P}"

# Server init.d runscript
	einfo "Installing rabbit init.d script"
	newinitd ${FILESDIR}/${PV}/init.d/${PN}.initd ${PN} || die "failed to install init.d script for ${P}"

# Log rotation script
	einfo "Installing rabbit logrotate configuration"
	insinto /etc/logrotate.d/
	doins ${FILESDIR}/${PV}/logrotate.d/${PN} || die "failed to install logrotate.d file for ${P}"

# Log directory
	dodir "/var/log/rabbitmq"	\
		|| die "failed to create log directory for ${P}"

	dodir  /var/lib/rabbitmq  \
		|| die "couldn't create mnesia home"
	
# mnesia
	einfo "fixing user permissions for rabbitmq"
	fperms  700 /var/lib/rabbitmq  \
		|| die "couldn't chmod mnesia home"
	
	fowners rabbitmq:rabbitmq /var/lib/rabbitmq \
		|| die "couldn't chown mnesia home"

# rabbit logs
	einfo "fixing user permissions for rabbitmq logs"
	fperms  700 /var/log/rabbitmq \
		|| die "couldn't chmod rabbitmq log base"
	
	fowners rabbitmq:rabbitmq /var/log/rabbitmq \
		|| die "couldn't chown rabbitmq log base"

# rabbit home
	einfo "fixing user permissions for rabbitmq home"
	dodir /var/tmp/rabbitmq \
		|| die "couldn't create rabbitmq home"
	fperms  700 /var/tmp/rabbitmq \
		|| die "couldn't chmod rabbitmq home"
	
	fowners rabbitmq:rabbitmq /var/tmp/rabbitmq \
		|| die "couldn't chown rabbitmq home"
}

unpack()
{
	unpack ${A}		\
		|| die "failed to unpack ${A}"

}

src_compile()
{
	einfo "Compiling rabbitmq-server"
	cd "${S}"
	# fix: change script includes to use files in /etc/conf.d
	epatch ${FILESDIR}/${PV}/patches/0001-change-conf-dir.patch \
		|| die "failed to patch  ${S}"
	emake clean || die "failed to clean ${P}"
	emake || die "failed to make ${P}"
}

pkg_setup()
{
	# add rabbitmq user and group so we can run as a nologin user
	einfo "adding rabbitmq group"
	enewgroup rabbitmq	\
		|| die "couldn't create rabbitmq group"

	# rabbit requires a writeable home directory 
	einfo "adding rabbitmq user"
	enewuser rabbitmq -1 -1 /var/tmp/rabbitmq rabbitmq \
		|| die "couldn't create rabbitmq user"
}

pkg_postinst()
{
	# tell user this is not an offical ebuild	
	ewarn "IMPORTANT:"
	ewarn "This is an unofficial ebuild for RabbitMQ (server) "
	ewarn "If you encounter any problems, do NOT file bugs to gentoo"
	ewarn "bugzilla. Instead, post into this ebuild's topic on the"
	ewarn "Gentoo Bugzilla list"
	ewarn
	ewarn "link:"
	ewarn "http://bugs.gentoo.org/show_bug.cgi?id=192278"

	# explain how to run as daemon
	elog "You can configure RabbitMQ to run as a daemon by running:"
	elog
	elog "rc-update add rabbitmq-server default"
	elog
}
