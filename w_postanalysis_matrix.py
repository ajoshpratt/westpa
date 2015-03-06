from __future__ import print_function, division; __metaclass__ = type

import sys, logging
from collections import deque

import os
file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(file_dir)

import numpy as np
import numba as nb
import scipy.sparse as sp

import westpa
from westpa import h5io
from west.data_manager import weight_dtype
from west.data_manager import seg_id_dtype
from westpa.binning import index_dtype
from westpa.kinetics._kinetics import _fast_transition_state_copy #@UnresolvedImport
from westpa.kinetics.matrates import estimate_rates
from westtools import (WESTTool, WESTDataReader, IterRangeSelection,
                       ProgressIndicatorComponent)


log = logging.getLogger('westtools.w_calc_postanalysis_matrix')


class MatrixRw(WESTTool):
    '''Base class for common options for both kinetics schemes'''
    
    def __init__(self):
        super(MatrixRw, self).__init__()
        self.progress = ProgressIndicatorComponent()
        self.data_reader = WESTDataReader()
        self.iter_range = IterRangeSelection() 
        self.output_file = None
        self.assignments_file = None
        self.default_output_file = 'flux_matrices.h5'
        self.window_size = None
        

    def add_args(self, parser):
        self.data_reader.add_args(parser)
        self.iter_range.add_args(parser)
        
        iogroup = parser.add_argument_group('input/output options')
        iogroup.add_argument('-a', '--assignments', default='assign.h5',
                             help='''Bin assignments and macrostate definitions are in ASSIGNMENTS
                                (default: %(default)s).''')

        iogroup.add_argument('-o', '--output', dest='output', default=self.default_output_file,
                             help='''Store results in OUTPUT (default: %(default)s).''')
        iogroup.add_argument('--colors-from-macrostates', action='store_true',
                             help='''Construct the color assignments from macrostate labels in the
                             assignment file. Otherwise raw bin assignments are assumed to contain
                             history information.''')

        self.progress.add_args(parser)
        
    def process_args(self, args):
        self.progress.process_args(args)
        self.assignments_file = h5io.WESTPAH5File(args.assignments, 'r')
        self.data_reader.process_args(args)
        with self.data_reader:
            self.iter_range.process_args(args)
        self.output_file = h5io.WESTPAH5File(args.output, 'w', creating_program=True)
        h5io.stamp_creator_data(self.output_file)
        if not self.iter_range.check_data_iter_range_least(self.assignments_file):
            raise ValueError('assignments do not span the requested iterations')

        self.colors_from_macrostates = args.colors_from_macrostates
        
    def go(self):
        pi = self.progress.indicator
        pi.new_operation('Initializing')
        with pi:
            self.data_reader.open('r')
            nbins = self.assignments_file.attrs['nbins']

            if self.colors_from_macrostates:
                if 'state_map' in self.assignments_file:
                    state_labels = self.assignments_file['state_labels'][...]
                    state_map = self.assignments_file['state_map'][...]
                    nstates = len(state_labels)
                else:
                    raise ValueError('Assignment file does not contain macrostate data')

            start_iter, stop_iter = self.iter_range.iter_start, self.iter_range.iter_stop # h5io.get_iter_range(self.assignments_file)
            iter_count = stop_iter - start_iter

            if self.colors_from_macrostates:
                nfbins = nbins * nstates
            else:
                nfbins = nbins


            flux_shape = (iter_count, nfbins, nfbins)
            pop_shape = (iter_count, nfbins)

            h5io.stamp_iter_range(self.output_file, start_iter, stop_iter)

            bin_populations_ds = self.output_file.create_dataset('bin_populations', shape=pop_shape, dtype=weight_dtype)
            h5io.stamp_iter_range(bin_populations_ds, start_iter, stop_iter)
            h5io.label_axes(bin_populations_ds, ['iteration', 'bin'])

            flux_grp = self.output_file.create_group('iterations')
            self.output_file.attrs['nrows'] = nfbins
            self.output_file.attrs['ncols'] = nfbins

            fluxes = np.empty(flux_shape[1:], weight_dtype)
            populations = np.empty(pop_shape[1:], weight_dtype)
            trans = np.empty(flux_shape[1:], np.int64)

            pi.new_operation('Calculating flux matrices', iter_count)
            # Calculate instantaneous statistics
            for iiter, n_iter in enumerate(xrange(start_iter, stop_iter)):
                # Get data from the main HDF5 file
                iter_group = self.data_reader.get_iter_group(n_iter)
                seg_index = iter_group['seg_index']
                nsegs, npts = iter_group['pcoord'].shape[0:2] 
                weights = seg_index['weight']

                # Get bin and traj. ensemble assignments from the previously-generated assignments file
                assignment_iiter = h5io.get_iteration_entry(self.assignments_file, n_iter)
                bin_assignments = np.require(self.assignments_file['assignments'][assignment_iiter + np.s_[:nsegs,:npts]],
                                                dtype=index_dtype)

                mask_unknown = np.zeros_like(bin_assignments, dtype=np.uint16)

                # Get macrostate/color assignments if not excluded explicitly in the bin assignments
                if self.colors_from_macrostates:
                    macrostate_iiter = h5io.get_iteration_entry(self.assignments_file, n_iter)
                    macrostate_assignments = np.require(self.assignments_file['trajlabels'][macrostate_iiter + np.s_[:nsegs,:npts]],
                                                dtype=index_dtype)

                    # Transform bin_assignments to take macrostate membership into account
                    bin_assignments  = nstates * bin_assignments + macrostate_assignments
                    mask_indx = np.where(macrostate_assignments == nstates)
                    mask_unknown[mask_indx] = 1


                # Calculate bin-to-bin fluxes, bin populations and number of obs transitions
                calc_stats(bin_assignments, weights, fluxes, populations, trans, mask_unknown)


                # Store bin-based kinetics data
                bin_populations_ds[iiter] = populations

                # Setup sparse data structures for flux and obs
                fluxes_sp = sp.coo_matrix(fluxes)
                trans_sp = sp.coo_matrix(trans)

                assert fluxes_sp.nnz == trans_sp.nnz

                flux_iter_grp = flux_grp.create_group('iter_{:08d}'.format(n_iter))
                flux_iter_grp.create_dataset('flux', data=fluxes_sp.data, dtype=weight_dtype)
                flux_iter_grp.create_dataset('obs', data=trans_sp.data, dtype=np.int32)
                flux_iter_grp.create_dataset('rows', data=fluxes_sp.row, dtype=np.int32)
                flux_iter_grp.create_dataset('cols', data=fluxes_sp.col, dtype=np.int32)
                flux_iter_grp.attrs['nrows'] = nfbins
                flux_iter_grp.attrs['ncols'] = nfbins

                # Do a little manual clean-up to prevent memory explosion
                del iter_group, weights, bin_assignments, macrostate_assignments
                pi.progress += 1


@nb.jit('void(u2[:,:], f8[:], f8[:,:], f8[:], i8[:,:], u2[:,:])', nopython=True)
def stats_process(bin_assignments, weights, fluxes, populations, trans, mask):
    nsegs = bin_assignments.shape[0]
    npts = bin_assignments.shape[1]

    for k in xrange(nsegs):
        ibin = bin_assignments[k,0]
        fbin = bin_assignments[k, npts-1]

        if mask[k, 0] == 1:
            continue

        w = weights[k]

        fluxes[ibin, fbin] += w
        trans[ibin, fbin] += 1
        populations[ibin] += w


def calc_stats(bin_assignments, weights, fluxes, populations, trans, mask):
    fluxes.fill(0.0)
    populations.fill(0.0)
    trans.fill(0)

    stats_process(bin_assignments, weights, fluxes, populations, trans, mask)

if __name__ == '__main__':
    MatrixRw().main()
