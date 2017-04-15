#!/usr/bin/env python
import numpy
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

def coord_loader(fieldname, coord_filename, segment, single_point=False):
    """
    Loads and stores coordinates

    **Arguments:**
        :*fieldname*:      Key at which to store dataset
        :*coord_filename*: Temporary file from which to load coordinates
        :*segment*:        WEST segment
        :*single_point*:   Data to be stored for a single frame
                           (should always be false)
    """
    # Load coordinates
    n_frames = 6
    n_atoms  = 2
    coord    = numpy.loadtxt(coord_filename, dtype = numpy.float32)
    coord    = numpy.reshape(coord, (n_frames, n_atoms, 3))

    # Save to hdf5
    segment.data[fieldname] = coord

def log_loader(fieldname, log_filename, segment, single_point=False):
    """
    Loads and stores log

    **Arguments:**
        :*fieldname*:    Key at which to store dataset
        :*log_filename*: Temporary file from which to load log
        :*segment*:      WEST segment
        :*single_point*: Data to be stored for a single frame
                         (should always be false)
    """
    # Load log
    with open(log_filename, 'r') as log_file:
        raw_text = [line.strip() for line in log_file.readlines()]

    # Determine number of fields
    n_frames = 6
    n_fields = 0
    line_i   = 0
    starts   = []
    while line_i < len(raw_text):
        line = raw_text[line_i]
        start = line.split()[0]
        if start in starts:
            break
        else:
            starts.append(start)
        n_fields += line.count('=')
        line_i   += 1
    dataset = numpy.zeros((n_frames, n_fields), numpy.float32)

    # Parse data
    line_i  = 0
    frame_i = 0
    field_i = 0
    while line_i < len(raw_text):
        line = raw_text[line_i]
        for field in line.split():
            try:
                float(field)
                dataset[frame_i, field_i] = float(field)
                if field_i == n_fields - 1:
                    frame_i += 1
                    field_i  = 0
                else:
                    field_i += 1
            except ValueError:
                pass
        line_i += 1

    # Save to hdf5
    segment.data[fieldname] = dataset
