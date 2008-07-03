#! /bin/sh

# We check for the presence of the tools necessary to build a release on a
# Debian based OS.

TOOLS_STOP=0

checker () {
	if [ ! `which $1` ]
	then
		echo "$1 is missing, please install it" 
		TOOLS_STOP=1 
		NEW_NAME=`echo $1 | sed -e 's/-/_/g'`
		eval "$NEW_NAME=1" 
	else
		echo "$1 found"
	fi
};

echo ~~~~~~~~~~~~ Looking for mandatory programs ~~~~~~~~~~~~ 

for i in cdbs-edit-patch reprepro rpm elinks wget zip gpg rsync
do
  checker $i
done
echo ~~~~~~~~~~~~~~~~~~~~~~~~~~ DONE  ~~~~~~~~~~~~~~~~~~~~~~~ 

if [ 1 = $TOOLS_STOP ] 
then
	[ $cdbs_edit_patch ] && cdbs_edit_patch="cdbs "
	[ $reprepro ] && reprepro="reprepro "
	[ $rpm ] && rpm="rpm "
	[ $elinks ] && elinks="elinks "
	[ $wget ] && wget="wget "
	[ $zip ] && zip="zip "
	[ $gpg ] && gpg="gpg "
	[ $rsync ] && rsync="rsync "

	echo
	echo We suggest you run the command
	echo "apt-get install ${cdbs_edit_patch}${reprepro}${rpm}${elinks}${wget}${zip}${gpg}${rsync}"
	echo
fi

exit $TOOLS_STOP
