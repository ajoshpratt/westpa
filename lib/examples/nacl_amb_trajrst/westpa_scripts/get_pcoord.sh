#!/bin/bash

set -x
env | sort
if [ -n "$SEG_DEBUG" ] ; then
    set -x
    env | sort
fi

#cd $WEST_SIM_ROOT

cp $WEST_STRUCT_DATA_REF $WEST_TRAJECTORY_RETURN
cp -vr $WEST_STRUCT_DATA_REF $WEST_RESTART_RETURN/parent.rst
cp -vr amber_config/nacl.prm $WEST_RESTART_RETURN/nacl.prmtop
wait

if [ -n "$SEG_DEBUG" ] ; then
    head -v $WEST_PCOORD_RETURN
fi
exit 0
