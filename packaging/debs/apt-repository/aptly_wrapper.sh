#!/bin/bash

# aptly_wrapper.sh

# 2014.10.08: preserve hardlinks with rsync, aptly requires them

# Commenting set -e, as it will cause the script to bail when handling
# certain errors, such as a non-exisiting remote aptly dir when performing
# a fetch.
#set -e

DEBUG=yes

DATE=`date +%y%d-%H%M%S`

# repo name, much like Debian's distribution name of Wheezy, Jessie, etc
REPO=kitten
# Similar to Debian's use of pointing stable, testing, etc to distributions
# though here, testing is meant to imply frequent releases.
# see bug25313 for explanation.
LINKED_REPO=testing

REPO_COMMENT="RabbitMQ Repository for Debian / Ubuntu etc"
# Pulled these from the reprepro distributions conf file
REPO_COMPONENT=main
REPO_LABEL="RabbitMQ Repository for Debian / Ubuntu etc"
REPO_ORIGIN=RabbitMQ

# These would ideally come from the release.mk
# Need to determine actual dir layout for prod server where we can put
#  aptly/
DEPLOY_HOST=
DEPLOY_PATH=/tmp/rabbitmq/extras/releases
DEPLOY_DEST=${DEPLOY_HOST}:${DEPLOY_PATH}

# Rsync needs to preserve hardlinks which Aptly uses to map packages in the
# metadata pool with the public pool
RSYNC_CMD="rsync -rpltH --delete-after --itemize-changes"

APTLY_CONF=aptly.conf  # packaging/debs/apt-repository/
APTLY="aptly -config=${APTLY_CONF}"

Debug() {
   # Debug "this will appear when debug = yes only"
   if [ "$DEBUG" = "yes" ]; then
      echo $*
   fi
}

Usage() {
   echo ""
   echo "`basename $0` -o [add|publish|fetch|push|diff|snap-pub-testing] -h GPG_HOME -g GPG_KEY_ID -f \"pkg_full_name another_pkg_full_name\""
   echo ""
   echo "For example:"
   echo "     To pull the persistence files from the server:"
   echo "     ./`basename $0` -o fetch"
   echo ""
   echo "     To add a package to a repo:"
   echo "     ./`basename $0` -o add -f rabbitmq-server_3.3.5-1_all.deb"
   echo ""
   echo "     To add multiple packages to a repo:"
   echo "     ./`basename $0` -o add -f \"rabbitmq-server_3.3.5-1_all.deb rabbitmq-server_3.3.5-1.dsc\""
   echo ""
   echo "     To publish the repo:"
   echo "     ./`basename $0` -o publish -h HOME=/tmp -g 57F929F7"
   echo ""
   echo "     To push the persistence files and public repo data to server:"
   echo "     ./`basename $0` -o push"
   echo ""
   exit
}

# Minimum cmd line options
if [ "$#" -lt "2" ]; then
   Usage
fi


while getopts ":o:f:g:h:" opt; do
   case $opt in
       o  ) OP=$OPTARG ;;
       f  ) PKG_NAME_FULL=$OPTARG ;;
       g  ) GPG_KEY=$OPTARG ;;
       h  ) GPG_HOME=$OPTARG ;;
       \? ) Usage ;;
   esac
done

if [ -z "$OP" ]; then
   echo ""
   echo "The -o option is required"
   Usage
fi


case $OP in
   fetch)
      # Pull via ssh the Aptly persistence files from the server

      # Check existence of remote persistence files expected to be in the
      # aptly directory.
      APTLY_DIR_EXISTS=`ssh ${DEPLOY_HOST} "ls $DEPLOY_PATH 2>/dev/null | grep -c aptly"`

      # Use -gt to handle existence of more than one instance of an aptly
      # named directory, such as aptly and aptly.old.
      if [ "$APTLY_DIR_EXISTS" -gt "0" ]; then
         echo "aptly dir does exist on $DEPLOY_HOST"
         Debug "$RSYNC_CMD ${DEPLOY_DEST}/aptly/* aptly/"
         $RSYNC_CMD ${DEPLOY_DEST}/aptly/* aptly/
      else 
         echo "aptly dir does NOT exist on $DEPLOY_HOST"
         echo "Nothing to fetch. Add operation will automatically create"
         echo "the repo."
      fi
      ;;

   diff)
         # perform a dry-run fetch of the remote repo with rsync so
         # itemize-changes can be used to compare the remote persistence
         # files to what is local.
         Debug "$RSYNC_CMD --dry-run ${DEPLOY_DEST}/aptly/* aptly/"
         $RSYNC_CMD --dry-run ${DEPLOY_DEST}/aptly/* aptly/
      ;;
   add)
      if [ -z "$PKG_NAME_FULL" ]; then
         echo ""
         echo "The full package name, with extension, is required for package"
         echo "add (-f rabbitmq-server_3.3.5-1_all.deb)"
         Usage
      fi
      # Check that the local repo exists prior to attempting to add a pkg to it
      if [ `$APTLY repo list | grep -c "No local repositories"` = "1" ]; then
         # No existing repos, create our repo
         $APTLY -comment="${REPO_COMMENT}" repo create $REPO 

         for PKGS in `echo "$PKG_NAME_FULL"`; do
            # Test that the package file exists
            if [ ! -f "$PKGS" ]; then
               echo "Package $PKGS does not exist"
               exit 1
            fi

            echo "Adding: $PKGS "
            $APTLY repo add $REPO $PKGS
         done
      else
         # Existing repo, check if the packages already exist in the repo
         # as attempting to add an existing pkg will cause an ugly error.
         for PKGS in `echo "$PKG_NAME_FULL"`; do
            # Test that the package file exists
            if [ ! -f "$PKGS" ]; then
               echo "Package $PKGS does not exist"
               exit 1
            fi
            # Need to check the PKGS against what packages may already
            # be in the repo. The PKGS var needs to have any path or
            # suffix removed first to match the output of repo show.
            MOD_PKG_NAME=`echo $PKGS | awk -F'/' '{print $NF}' | sed -e 's/.deb//' -e 's/.dsc/_source/'` 
            if [ `$APTLY repo show -with-packages $REPO | grep -c $MOD_PKG_NAME` -gt "0" ]; then
               # Pack already exists in repo, do not attempt to add it
               echo ""
               echo "Skipping add, already in repo: $PKGS"
            else
               echo "Adding: $PKGS "
               $APTLY repo add $REPO $PKGS
            fi
         done
      fi 

      ;;
   publish)
      if [ -z "$GPG_KEY" ]; then
         # assume this is an UNOFFICIAL_RELEASE and the repo will not be
         # signed. A further assumption is that gpgDisableSign has been
         # set to true in the aptly.conf file by the Makefile.
         if [ `$APTLY publish list | grep -c ^"No snapshots/local"` = "1" ]; then
            $APTLY -component="${REPO_COMPONENT}" \
           -label="${REPO_LABEL}" -origin="${REPO_ORIGIN}" \
           -distribution="${REPO}" publish repo $REPO debian
         else
            $APTLY publish update ${REPO} debian
         fi
      else
         # Assume this is a release build that will be signed
  
         # Check if the repo has been previously published because the commands
         # are different for an initial publish vs an update to an existing
         # published repo.
         if [ `$APTLY publish list | grep -c ^"No snapshots/local"` = "1" ]; then
            # initial publish with origin, label, component and distribution.
            # Note that the final option, debian, is the prefix for the public
            # distributions which will be apt-repository/aptly/public/debian.
            if [ ! -z "$GPG_HOME" ]; then  
               export "$GPG_HOME"; $APTLY -gpg-key="${GPG_KEY}" \
                  -component="${REPO_COMPONENT}" \
                  -label="${REPO_LABEL}" -origin="${REPO_ORIGIN}" \
                  -distribution="${REPO}" publish repo $REPO debian
            else
               $APTLY -gpg-key="${GPG_KEY}" -component="${REPO_COMPONENT}" \
                  -label="${REPO_LABEL}" -origin="${REPO_ORIGIN}" \
                  -distribution="${REPO}" publish repo $REPO debian
            fi
         else
            # perform a publish update to the existing repo
            # the update option requires a distribution name but currently
            # the REPO value is the same as the distribution.
            # The last value, currently debian, is the prefix. 
            if [ ! -z "$GPG_HOME" ]; then  
               export "$GPG_HOME"; $APTLY -gpg-key="${GPG_KEY}" \
                  publish update ${REPO} debian
            else
               $APTLY -gpg-key="${GPG_KEY}" publish update ${REPO} debian
            fi
         fi
      fi
      ;;
   push)
      # Push via rsync over ssh the Aptly persistence files and public
      # repo data. 
      Debug "$RSYNC_CMD aptly ${DEPLOY_DEST}/"
      $RSYNC_CMD aptly ${DEPLOY_DEST}/
      ;;
   snap-pub-testing)
      # Snapshot kitten repo then publish it with the testing distribution
      # name to create the testing repo as a copy of kitten.

      # Create snapshot of repo
      SNAPSHOT_NAME=${LINKED_REPO}-${DATE}
      $APTLY snapshot create $SNAPSHOT_NAME from repo $REPO


      # If a gpg key was passed in, setup a variable for it
      if [ ! -z "$GPG_KEY" ]; then
         GPG_KEY_FLAG="-gpg-key=\"${GPG_KEY}\""
      fi
      # Has the initial snapshot been published before?
      # More specifically, has a snapshot been published to this 
      # distribution before?
      if [ `$APTLY publish list | grep -c debian/${LINKED_REPO}` -gt "0" ]; then
         # publish switch, the initial publish snapshot command has been run
         # switch the public files from the old one to this one
         if [ ! -z "$GPG_HOME" ]; then
            export "$GPG_HOME"; $APTLY publish switch $GPG_KEY_FLAG \
             ${LINKED_REPO} debian $SNAPSHOT_NAME
         else
            $APTLY publish switch $GPG_KEY_FLAG \
             ${LINKED_REPO} debian $SNAPSHOT_NAME
         fi
      else
         # publish snapshot, this is the initial publish for a snapshot
         if [ ! -z "$GPG_HOME" ]; then
            export "$GPG_HOME"; $APTLY publish snapshot \
             -distribution="${LINKED_REPO}" $GPG_KEY_FLAG $SNAPSHOT_NAME debian
         else
            $APTLY publish snapshot \
             -distribution="${LINKED_REPO}" $GPG_KEY_FLAG $SNAPSHOT_NAME debian
         fi
      fi


      ;;
esac

# TODO make the aptly prefix, currently debian, to be a variable and describe
# its purpose.
