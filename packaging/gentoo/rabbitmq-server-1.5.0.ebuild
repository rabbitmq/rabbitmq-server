# Copyright 1999-2008 Gentoo Foundation
# Distributed under the terms of the GNU General Public License v2
# $Header: $

inherit eutils

DESCRIPTION="RabbitMQ is a high-performance AMQP-compliant message broker written in Erlang."
HOMEPAGE="http://www.rabbitmq.com/" 
SRC_URI="http://www.rabbitmq.com/releases/rabbitmq-server/v${PV}/rabbitmq-server-generic-unix-${PV}.tar.gz"
LICENSE="MPL"
SLOT="0" 
KEYWORDS="~alpha ~amd64 ~ppc ~ppc64 ~sparc ~x86"
IUSE=""
 
# Q: is RDEPEND-only sufficient for a binary package, since we don't compile?
DEPEND="dev-lang/erlang"
RDEPEND="${DEPEND}"

# grr: the packaged directory contains an underscore
MODNAME="rabbitmq_server-${PV}"
S="${WORKDIR}/${MODNAME}"

src_install() {
	# erlang module
	local targetdir="/usr/$(get_libdir)/erlang/lib/${MODNAME}"
	dodir "${targetdir}"
	cp -dpR ebin include "${D}/${targetdir}"

	# scripts
	dosbin sbin/*

	# docs
	dodoc INSTALL LICENSE LICENSE-MPL-RabbitMQ

	# TODO:
	# config to set env vars as per INSTALL?
	# set LOGDIR to /var/log/rabbitmq.log
	# run as different user?
}
