import mdtraj as md
import h5py
import westpa
import StringIO
import tempfile
import os


def pcoord_loader(fieldname, pcoord_return_filename, segment, single_point, trajectory, restart):
    """Read progress coordinate data into the ``pcoord`` field on ``destobj``. 
    An exception will be raised if the data is malformed.  If ``single_point`` is true,
    then only one (N-dimensional) point will be read, otherwise system.pcoord_len points
    will be read.
    """
    
    system = westpa.rc.get_system_driver()
    
    assert fieldname == 'pcoord'

    # Load the file!  Notice that we're just loading the trajectory, not the pcoord file itself.
    # for get bstates
    try:
        if single_point:
            b = md.load_restrt(trajectory, top=os.path.join(restart, 'nacl.prmtop'))
        else:
            b = md.load_netcdf(trajectory, top=os.path.join(restart, 'nacl.prmtop'))
    except:
        segment.pcoord = 1
    else:
        # Do it from atoms 1 and 2.
        pcoord = md.compute_distances(b, [[0,1]]) * 10
        #print(pcoord)
        
        if single_point:
            expected_shape = (system.pcoord_ndim,)
            pcoord = pcoord[0]
            if pcoord.ndim == 0:
                pcoord.shape = (1,)
        else:
            expected_shape = (system.pcoord_len, system.pcoord_ndim)
            if pcoord.ndim == 1:
                pcoord.shape = (len(pcoord),1)
        if pcoord.shape != expected_shape:
            raise ValueError('progress coordinate data has incorrect shape {!r} [expected {!r}]'.format(pcoord.shape,
                                                                                                        expected_shape))
        segment.pcoord = pcoord
