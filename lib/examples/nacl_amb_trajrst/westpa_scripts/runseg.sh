#!/bin/bash

if [ -n "$SEG_DEBUG" ] ; then
    set -x
    env | sort
fi

case $WEST_CURRENT_SEG_INITPOINT_TYPE in
    SEG_INITPOINT_CONTINUES)
        # A continuation from a prior segment
        # $WEST_PARENT_DATA_REF contains the reference to the
        #   parent segment
        sed "s/RAND/$WEST_RAND16/g" \
          $WEST_SIM_ROOT/amber_config/md-continue.in > md.in
    ;;

    SEG_INITPOINT_NEWTRAJ)
        # Initiation of a new trajectory
        # $WEST_PARENT_DATA_REF contains the reference to the
        #   appropriate basis or initial state
        sed "s/RAND/$WEST_RAND16/g" \
          $WEST_SIM_ROOT/amber_config/md-genvel.in > md.in
    ;;

    *)
        echo "unknown init point type $WEST_CURRENT_SEG_INITPOINT_TYPE"
        exit 2
    ;;
esac

# Propagate segment
$PMEMD -O -i md.in   -p nacl.prmtop  -c parent.rst \
          -r seg.rst -x seg.crd      -o seg.log    -inf seg.nfo

# Send in the coordinates and the pcoord.
cp seg.crd $WEST_TRAJECTORY_RETURN

# Send in the information necessary to restart the next simulation.
cp seg.rst $WEST_RESTART_RETURN/parent.rst
cp nacl.prmtop $WEST_RESTART_RETURN
