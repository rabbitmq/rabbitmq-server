Notes on creating rpms for rabbitmq

Assuming that rpm will be built under $TOP_DIR/rpm,
the main configuration variables would look like:

%_topdir $TOP_DIR/rpm
%_tmppath $TOP_DIR/rpm/tmp
%_bindir /usr/bin
%_libdir /usr/lib
%_includedir /usr/include
%_mandir /usr/share/man

Where $TOP_DIR can be any directory (default is $HOME).

The $TOP_DIR/rpm directory has following structure:

rpm
 +---- BUILD	// directory where tarballs are unpacked
 +---- SOURCES	// where source tarballs are put
 +---- SPECS	// directory containing specs
 +---- SRPMS	// rpmbuild puts here srpms
 +---- RPMS     // rpmbuils puts here rpms
 +---- tmp      // where rpm packages are built
 
Makefile will copy the source tarball from fixed directory
specified by $TARBALL_DIR to SOURCES directory and 
similarly specs from $SPEC_DIR to SPECS directory.
 
'make rpms' should create rabbitmq-server package.
If there are any errors reported by rpmbuild this is
possibly due to incorrect name of the packages
(if all dependencies are satisifed) - different distros
can have slightly different names.

rpms and srpms are placed in their respective directories.

'make prepare' will create the necessary structure.
Change main configuration variables specified in the 'DEFINES'
variable in the Makefile to adjust it to your system.
Note that it will *overwrite* any current rpmmacros
configurations.

The first thing to do for building rpms is to create you own
source tarball of AMQ. In the spec files two top variables
determine the name of the tarball. Adjust it to you needs.
The final name has to match the *Source* tag in spec's headers.

For information on how to sign the package see:
http://fedoranews.org/tchung/gpg/
