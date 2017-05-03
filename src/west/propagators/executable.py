# Copyright (C) 2013 Matthew C. Zwier and Lillian T. Chong
#
# This file is part of WESTPA.
#
# WESTPA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# WESTPA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with WESTPA.  If not, see <http://www.gnu.org/licenses/>.


import os, shutil, sys, signal, random, subprocess, time, tempfile
import numpy
import logging
from west.states import BasisState, InitialState
log = logging.getLogger(__name__)

# Get a list of user-friendly signal names
SIGNAL_NAMES = {getattr(signal, name): name for name in dir(signal) 
                if name.startswith('SIG') and not name.startswith('SIG_')}

import westpa
from westpa.extloader import get_object
from westpa.yamlcfg import check_bool, ConfigItemMissing
import west
from west import Segment
from west.propagators import WESTPropagator
from west import errors
from west.data_manager import WESTDataManager
import tarfile, StringIO, os, io, cStringIO
import cPickle
import h5py
import traceback
vvoid_dtype = h5py.special_dtype(vlen=str) # Trying to store arbitrary data.  Not working so well...

# We're using functions that want this, so.
error = errors.WESTErrorReporting(sys.argv[0])

import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=RuntimeWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

def pcoord_loader(fieldname, pcoord_return_filename, destobj, single_point, **kwargs):
    """Read progress coordinate data into the ``pcoord`` field on ``destobj``. 
    An exception will be raised if the data is malformed.  If ``single_point`` is true,
    then only one (N-dimensional) point will be read, otherwise system.pcoord_len points
    will be read.
    """
    
    system = westpa.rc.get_system_driver()
    
    assert fieldname == 'pcoord'
    
    try:
        pcoord = numpy.loadtxt(pcoord_return_filename, dtype=system.pcoord_dtype)
    except:
        # We failed to properly use numpy loadtxt.  This isn't because it's empty, but because it's probably malformed.
        #error.report_segment_error(error.PCOORD_LOADER_ERROR, segment=destobj, err=error.format_stderr(destobj.err))
        destojb.error.append(error.report_segment_error(error.LOADTXT_ERROR, dataset=fieldname, segment=destobj, err=error.format_stderr(destobj.err)))
        #error.raise_exception()


    destobj.pcoord = pcoord

def check_pcoord(destobj, single_point, original_pcoord, executable=None, logfile=None, **kwargs):
    # A function to check whether out pcoord is correctly shaped.
    system = westpa.rc.get_system_driver()

    # if it fails here, it's totally screwed up.
    #try:
    pcoord = destobj.pcoord.copy()
    #else:
    #    pcoord = numpy.array(destobj.pcoord)

    if numpy.all(pcoord == original_pcoord):
        # Actually, it's not been updated.  We should handle this more appropriately.
        destobj.error.append(error.report_segment_error(error.EMPTY_PCOORD_ERROR, segment=destobj, err=error.format_stderr(destobj.err), executable=os.path.expandvars(executable), logfile=os.path.expandvars(logfile)))
        destobj.status = Segment.SEG_STATUS_FAILED
        #error.raise_exception()

    if single_point:
        expected_shape = (system.pcoord_ndim,)
        if pcoord.ndim == 0:
            pcoord.shape = (1,)
    else:
        expected_shape = (system.pcoord_len, system.pcoord_ndim)
        if pcoord.ndim == 1:
            pcoord.shape = (len(pcoord),1)

    if pcoord[0] == None:
        destobj.error.append(error.report_segment_error(error.EMPTY_PCOORD_ERROR, segment=destobj, err=error.format_stderr(destobj.err), executable=os.path.expandvars(executable), logfile=os.path.expandvars(logfile)))
        destobj.status = Segment.SEG_STATUS_FAILED
        #error.raise_exception()
            
    if pcoord.shape != expected_shape:
        if pcoord.shape == (0,1):
            destobj.error.append(error.report_segment_error(error.RUNSEG_PCOORD_ERROR, segment=destobj, err=error.format_stderr(destobj.err), pcoord=pcoord))
            destobj.status = Segment.SEG_STATUS_FAILED
            #error.raise_exception()
        elif pcoord.shape == (0,):
            # Failure on single points.  Typically for istate/bstates.
            destobj.error.append(error.report_segment_error(error.RUNSEG_PCOORD_ERROR, segment=destobj, err=error.format_stderr(destobj.err), executable=os.path.expandvars(executable), logfile=os.path.expandvars(logfile), pcoord=pcoord))
            destobj.status = Segment.SEG_STATUS_FAILED
            #error.raise_exception()
        else:
            destobj.error.append(error.report_segment_error(error.RUNSEG_SHAPE_ERROR, segment=destobj, shape=pcoord.shape, pcoord=pcoord))
            destobj.status = Segment.SEG_STATUS_FAILED
            #error.raise_exception()
    try:
        if numpy.any(numpy.isnan(pcoord)):
           # We can't have NaN in the pcoord.  Fail out.
            destobj.error.append(error.report_segment_error(error.RUNSEG_PCOORD_ERROR, segment=destobj, err=error.format_stderr(destobj.err), pcoord=pcoord))
            destobj.status = Segment.SEG_STATUS_FAILED
            #error.raise_exception()
    except:
       # If we can't check for a NaN, there are problems.
        destobj.error.append(error.report_segment_error(error.RUNSEG_PCOORD_ERROR, segment=destobj, err=error.format_stderr(destobj.err), pcoord=pcoord))
        destobj.status = Segment.SEG_STATUS_FAILED
        #error.raise_exception()
        #destobj.status = SEG_STATUS_FAILED

    #log.debug('{segment.seg_id} passed the pcoord check.'.format(segment=destobj))
    # This has properly shaped it, so.
    destobj.pcoord = pcoord



def trajectory_input(fieldname, coord_file, segment, single_point):
    # We'll assume it's... for the moment, who cares, just pickle it.
    # Actually, it seems we need to store it as a void, since we're just using it as binary data.
    # See http://docs.h5py.org/en/latest/strings.html
    with open (coord_file, mode='rb') as file:
        try:
            data = numpy.void(file.read())
            segment.data['trajectories/{}'.format(fieldname)] = data
        except Exception as e:
        #if data.nbytes == 0:
            #log.warning('could not read any trajectory data for {}.  Disable trajectory storage in your config file to remove this warning.'.format(fieldname))
            a = traceback.format_exc()
            a = "\n        ".join(a.splitlines()[:])
            #error.report_general_error_once(error.EMPTY_TRAJECTORY, segment=segment)
            segment.error.append(error.report_segment_error(error.EMPTY_TRAJECTORY, segment=segment, filename=coord_file, dataset=fieldname, e=e, loader=trajectory_input, traceback=a))
            #error.report_general_error_once(error.EMPTY_TRAJECTORY, segment=segment, filename=coord_file, dataset=fieldname, e=e, loader=trajectory_input, traceback=a, see_wiki=False)
            pass
        #except TypeError as e:
            #print(e)
            # We're sending in an empty file.  That's okay for this.
        #    pass
    #del(data)

def size_format(filesize, n=0):
    if n == 0:
        btype = 'B'
    if n == 1:
        btype = 'KB'
    if n == 2:
        btype = 'MB'
    if n == 3:
        # Your restart data shouldn't be anywhere near this size in 2017.
        btype = 'GB'
    if filesize < 1024:
        return str('{:.2f} {}'.format(filesize, btype))
    if filesize > 1024:
        return size_format(float(filesize)/1024,n=n+1)

def restart_input(fieldname, coord_file, segment, single_point):
    # See http://docs.h5py.org/en/latest/strings.html
    # It's actually a directory, in this case.
    # We load and tar in memory, then pickle as a string, which is stored in the HDF5 file as a variable length string to ensure data integrity.
    # We're specifying the string encoding to ensure platform consistency.
    d = io.BytesIO()
    t = tarfile.open(mode='w:', fileobj=d)
    t.add(coord_file, arcname='.')
    # If it's greater than 2 MB, maybe log a warning.
    tarsize = len(d.getvalue())
    itarsize = 2*1024*1024
    try:
        # Just for convenient formatting of basis/istates.
        segment.seg_id = segment.state_id
        segment.n_iter = 'PREP'
    except:
        pass
    if tarsize > itarsize:
        #log.warning('{fieldname} has a filesize of {tarsize}; this may result in RAM intensive WESTPA runs.'.format(fieldname=fieldname,tarsize=size_format(tarsize)))
        segment.error.append(error.report_segment_error(error.LARGE_RESTART, segment=segment, size=size_format(tarsize), see_wiki=False))
        #error.report_general_error_once(error.LARGE_RESTART, segment=segment, size=size_format(tarsize), see_wiki=False)
    if len(t.getmembers()) <= 1:
        #log.warning('You have not supplied any {} data.  Disable restarts in your config file to remove this warning.'.format(fieldname))
        segment.error.append(error.report_segment_error(error.EMPTY_RESTART, segment=segment, see_wiki=False))
        #error.report_general_error_once(error.EMPTY_RESTART, segment=segment, see_wiki=False)
        #del(segment.data['trajectories/{}'.format(fieldname)])
    else:
        segment.data['trajectories/{}'.format(fieldname)] = numpy.array(cPickle.dumps((d.getvalue()), protocol=0).encode('base64'), dtype=vvoid_dtype)
    t.close()
    d.close()
    del(d,t)
    log.debug('{fieldname} with size {tarsize} for seg_id {segment.seg_id} successfully loaded in iter {segment.n_iter}.'.format(segment=segment, fieldname=fieldname, tarsize=tarsize))
    # We could enable some sort of debug for the prop, but this likely results in excessive memory usage during normal runs.
    #with tarfile.open(fileobj=e, mode='r') as t:
    #    for file in t.getmembers():
    #        if file.name != '.':
    #            try:
    #                assert file.size != 0
                    # It's not forbidden to place an empty file, but it probably means the run has failed.
    #                log.warning('{file.name} has a size of 0; your segment has failed, or the file is empty.  This is likely not the behavior you want.'.format(file=file))
    

def restart_output(tarball, segment):
    # See http://docs.h5py.org/en/latest/strings.html
    # We 'recreate' the tarball by decoding, unpickling, and then loading it up as a file object in memory.
    # We then untar to the location specified, and delete the restart data on the segment (as it is rather memory intensive).

    e = io.BytesIO(cPickle.loads(str(segment.restart).decode('base64')))
    with tarfile.open(fileobj=e, mode='r:') as t:
        t.extractall(path=tarball)
    del(segment.restart)
    log.debug('Restart for seg_id {segment.seg_id} successfully untarred in iter {segment.n_iter} .'.format(segment=segment))
    e.close()
        
    del(e,t)

def aux_data_loader(fieldname, data_filename, segment, single_point):
    data = numpy.loadtxt(data_filename)
    if data.nbytes == 0:
        #raise ValueError('could not read any data for {}'.format(fieldname))
        # We may wish to enable an environment in which everything is handled within python.
        # Ergo, perhaps this shouldn't immediately break; if the field isn't set properly, it'll break
        # down the line when it tries to store the data (which happens half the time anyway).
        #log.warning('could not read any data for {}'.format(fieldname))
        #error.report_segment_error(error.RUNSEG_AUX_ERROR, segment=segment, err=error.format_stderr(segment.err), dataset=fieldname)
        #error.report_segment_error(error.RUNSEG_TMP_ERROR, segment=segment, filename=data_filename, dataset=fieldname, e='')
        segment.error.append(error.report_segment_error(error.LOADTXT_ERROR, dataset=fieldname, segment=destobj, err=error.format_stderr(destobj.err)))
        error.raise_exception()
    else:
        segment.data[fieldname] = data
    
    
class ExecutablePropagator(WESTPropagator):
    ENV_CURRENT_ITER         = 'WEST_CURRENT_ITER'
    
    # Environment variables set during propagation
    ENV_CURRENT_SEG_ID       = 'WEST_CURRENT_SEG_ID'
    ENV_CURRENT_SEG_DATA_REF = 'WEST_CURRENT_SEG_DATA_REF'
    ENV_CURRENT_SEG_INITPOINT= 'WEST_CURRENT_SEG_INITPOINT_TYPE'
    ENV_PARENT_SEG_ID        = 'WEST_PARENT_ID'
    ENV_PARENT_DATA_REF      = 'WEST_PARENT_DATA_REF'
    
    # Environment variables set during propagation and state generation
    ENV_BSTATE_ID            = 'WEST_BSTATE_ID'
    ENV_BSTATE_DATA_REF      = 'WEST_BSTATE_DATA_REF'
    ENV_ISTATE_ID            = 'WEST_ISTATE_ID'
    ENV_ISTATE_DATA_REF      = 'WEST_ISTATE_DATA_REF'
    
    # Environment variables for progress coordinate calculation
    ENV_STRUCT_DATA_REF      = 'WEST_STRUCT_DATA_REF'
    
    # Set everywhere a progress coordinate is required
    ENV_PCOORD_RETURN        = 'WEST_PCOORD_RETURN'
    ENV_TRAJECTORY_RETURN    = 'WEST_TRAJECTORY_RETURN'
    ENV_RESTART_RETURN       = 'WEST_RESTART_RETURN'
    
    ENV_RAND16               = 'WEST_RAND16'
    ENV_RAND32               = 'WEST_RAND32'
    ENV_RAND64               = 'WEST_RAND64'
    ENV_RAND128              = 'WEST_RAND128'
    ENV_RANDFLOAT            = 'WEST_RANDFLOAT'
        
    def __init__(self, rc=None):
        super(ExecutablePropagator,self).__init__(rc)
            
        # A mapping of environment variables to template strings which will be
        # added to the environment of all children launched.
        self.addtl_child_environ = dict()
        
        # A mapping of executable name ('propagator', 'pre_iteration', 'post_iteration') to 
        # a dictionary of attributes like 'executable', 'stdout', 'stderr', 'environ', etc.
        self.exe_info = {}
        self.exe_info['propagator'] = {}
        self.exe_info['pre_iteration'] = {}
        self.exe_info['post_iteration'] = {}
        self.exe_info['get_pcoord'] = {}
        self.exe_info['gen_istate'] = {}
        
        # A mapping of data set name ('pcoord', 'coord', 'com', etc) to a dictionary of
        # attributes like 'loader', 'dtype', etc
        # We want the pcoord last in this case, so ordereddict it is!
        from collections import OrderedDict
        self.data_info = OrderedDict()
        self.data_info['pcoord'] = {}

        # Validate configuration 
        config = self.rc.config
        
        # We absolutely need these keys.
        for key in [('west','executable','propagator','executable'),
                    ('west','data','data_refs','basis_state'),
                    ('west','data','data_refs','initial_state')]:
            config.require(key)

        self.cleanup = config['west', 'executable', 'propagator', 'cleanup'] if ('west', 'executable', 'propagator', 'cleanup') in config else True
        # These keys aren't mutually exclusive, but we do require at least one of them.
        if ('west','data','data_refs','segment') in config:
            pass
            # If we're using the older style, we don't want to automatically delete things.
            log.debug('Utilizing segment directory style: {}'.format(config[('west', 'data', 'data_refs', 'segment')]))
            self.cleanup = False
        else:
            config.require(('west','data','data_refs','seg_rundir'))
            config.require(('west','data','data_refs','trajectories'))
            log.debug('Utilizing segment directory style: {}'.format(os.path.join(config[('west', 'data', 'data_refs', 'seg_rundir')], '{segment.n_iter:06d}/{segment.seg_id:06d}')))
            self.segment_rundir             = config['west','data','data_refs','seg_rundir']

        

 
        #self.segment_ref_template       = self.segment_rundir + '/{segment.n_iter:06d}/{segment.seg_id:06d}'
        self.segment_ref_template       = config['west','data','data_refs','segment'] if ('west', 'data', 'data_refs', 'segment') in config else os.path.join(self.segment_rundir, '{segment.n_iter:06d}/{segment.seg_id:06d}')
        self.basis_state_ref_template   = config['west','data','data_refs','basis_state']
        self.initial_state_ref_template = config['west','data','data_refs','initial_state']
        #self.trajectory_types           = config['west','data','data_refs','trajectory_type'] 
        # Assume old style.
        if ('west','data','data_refs','segment') in config:
            do_restart = False
        else:
            do_restart = True
        
        # Load additional environment variables for all child processes
        self.addtl_child_environ.update({k:str(v) for k,v in (config['west','executable','environ'] or {}).iteritems()})
        
        
        # Load configuration items relating to child processes
        for child_type in ('propagator', 'pre_iteration', 'post_iteration', 'get_pcoord', 'gen_istate'):
            child_info = config.get(['west','executable',child_type])
            if not child_info:
                continue
            
            info_prefix = ['west', 'executable', child_type]
            
            # require executable to be specified if anything is specified at all
            config.require(info_prefix+['executable'])
            
            self.exe_info[child_type]['executable'] = child_info['executable']
            self.exe_info[child_type]['stdin']  = child_info.get('stdin', os.devnull)
            self.exe_info[child_type]['stdout'] = child_info.get('stdout', None)
            self.exe_info[child_type]['stderr'] = child_info.get('stderr', None)
            self.exe_info[child_type]['cwd'] = child_info.get('cwd', None)
            
            if child_type not in ('propagator', 'get_pcoord', 'gen_istate'):
                self.exe_info[child_type]['enabled'] = child_info.get('enabled',True)
            else:
                # for consistency, propagator, get_pcoord, and gen_istate can never be disabled
                self.exe_info[child_type]['enabled'] = True
            
            # apply environment modifications specific to this executable
            self.exe_info[child_type]['environ'] = {k:str(v) for k,v in (child_info.get('environ') or {}).iteritems()}
            
        log.debug('exe_info: {!r}'.format(self.exe_info))
        
        # Load configuration items relating to dataset input
        self.data_info['pcoord'] = {'name': 'pcoord',
                                    'loader': pcoord_loader,
                                    'enabled': True,
                                    'filename': None}
        self.data_info['trajectory'] = {'name': 'auxdata/trajectories/trajectory',
                                    'loader': trajectory_input,
                                    'delram': True,
                                    'enabled': True,
                                    'filename': None}
        # This is for stuff like restart files, etc.  That is, the things we'll need to continue the simulation.
        # For now, tar it, pickle it, and call it a day.
        # Then we untar, unpickle, and go from there.
        import h5py
        self.data_info['restart'] =  {'name': 'auxdata/trajectories/restart',
                                    'loader': restart_input,
                                    'delram': True,
                                    'enabled': do_restart,
                                    'filename': None,
                                    'dtype': h5py.new_vlen(str)}
        dataset_configs = config.get(['west', 'executable', 'datasets']) or []
        for dsinfo in dataset_configs:
            loader = None
            try:
                dsname = dsinfo['name']
            except KeyError:
                raise ValueError('dataset specifications require a ``name`` field')
            
            if dsname != 'pcoord':
                check_bool(dsinfo.setdefault('enabled', True))
            else:
                # can never disable pcoord collection
                dsinfo['enabled'] = True
            #if dsname == 'auxdata/trajectories/restart' or dsname == 'auxdata/trajectories/trajectory':
            #    dsinfo['delram'] == True
            
            loader_directive = dsinfo.get('loader')
            if loader_directive:
                loader = get_object(loader_directive)
            elif dsname != 'pcoord' and dsname != 'restart' and dsname != 'trajectory':
                loader = aux_data_loader
            
            if loader:
                dsinfo['loader'] = loader
            self.data_info.setdefault(dsname,{}).update(dsinfo)
            del(loader)
                                                    
        log.debug('data_info: {!r}'.format(self.data_info))
                
    @staticmethod                        
    def makepath(template, template_args = None,
                  expanduser = True, expandvars = True, abspath = False, realpath = False):
        template_args = template_args or {}
        path = template.format(**template_args)
        if expandvars: path = os.path.expandvars(path)
        if expanduser: path = os.path.expanduser(path)
        if realpath:   path = os.path.realpath(path)
        if abspath:    path = os.path.abspath(path)
        path = os.path.normpath(path)
        return path

    def random_val_env_vars(self):
        '''Return a set of environment variables containing random seeds. These are returned
        as a dictionary, suitable for use in ``os.environ.update()`` or as the ``env`` argument to
        ``subprocess.Popen()``. Every child process executed by ``exec_child()`` gets these.'''
        
        return {self.ENV_RAND16:               str(random.randint(0,2**16)),
                self.ENV_RAND32:               str(random.randint(0,2**32)),
                self.ENV_RAND64:               str(random.randint(0,2**64)),
                self.ENV_RAND128:              str(random.randint(0,2**128)),
                self.ENV_RANDFLOAT:            str(random.random())}
        
    def exec_child(self, executable, environ=None, stdin=None, stdout=None, stderr=None, cwd=None):
        '''Execute a child process with the environment set from the current environment, the
        values of self.addtl_child_environ, the random numbers returned by self.random_val_env_vars, and
        the given ``environ`` (applied in that order). stdin/stdout/stderr are optionally redirected.
        
        This function waits on the child process to finish, then returns
        (rc, rusage), where rc is the child's return code and rusage is the resource usage tuple from os.wait4()'''
        
        all_environ = dict(os.environ)
        all_environ.update(self.addtl_child_environ)
        all_environ.update(self.random_val_env_vars())
        all_environ.update(environ or {})
        
        stdin  = file(stdin, 'rb') if stdin else sys.stdin        
        stdout = file(stdout, 'wb') if stdout else sys.stdout
        if stderr == 'stdout':
            stderr = stdout
        else:
            stderr = file(stderr, 'wb') if stderr else sys.stderr
                
        # close_fds is critical for preventing out-of-file errors
        from subprocess import PIPE
        proc = subprocess.Popen([executable],
                                cwd = cwd,
                                #stdin=stdin, stdout=stdout, stderr=stderr if stderr != stdout else subprocess.STDOUT,
                                stdin=stdin, stdout=PIPE, stderr=PIPE,
                                close_fds=True, env=all_environ)

        # Wait on child and get resource usage
        # Oddly, we never fail with 0 as the integer option.  Need to look into this more.
        (_pid, _status, rusage) = os.wait4(proc.pid, 0)
        # Do a subprocess.Popen.wait() to let the Popen instance (and subprocess module) know that
        # we are done with the process, and to get a more friendly return code
        #rc = proc.wait()
        #return (rc, rusage)
        #rc = proc.wait()
        # While the return code is great, we may want to push more explicit error messages.
        # let's communicate and duplicate some of the stderr output, and send it on its way.
        # This may have to happen in the calling function, but whatever.
        out, err = proc.communicate()
        # Let's suppress writing this to the main log.  It clutters it up.
        if stdout != sys.stdout: 
            stdout.write(error.linebreak + ' STDOUT ' + error.linebreak + '\n\n\n')
            stdout.write(out)
            if stderr != stdout:
                stderr.write('\n\n\n' + error.linebreak + ' STDERR ' + error.linebreak + '\n\n\n')
                stderr.write(err)
            else:
                stdout.write('\n\n\n' + error.linebreak + ' STDERR ' + error.linebreak + '\n\n\n')
                stdout.write(err)
        rc = proc.returncode
        #return (rc, rusage, "\n        ".join(err.splitlines()[-10:]))
        return (rc, rusage, "\n        ".join(err.splitlines()[-10:]))
    
    def exec_child_from_child_info(self, child_info, template_args, environ):
        for (key, value) in child_info.get('environ', {}).iteritems():
            environ[key] = self.makepath(value)        
        return (environ, self.exec_child(executable = self.makepath(child_info['executable'], template_args),
                               environ = environ,
                               cwd = self.makepath(child_info['cwd'], template_args) if child_info['cwd'] else None,
                               stdin = self.makepath(child_info['stdin'], template_args) if child_info['stdin'] else os.devnull,
                               stdout= self.makepath(child_info['stdout'], template_args) if child_info['stdout'] else None,
                               stderr= self.makepath(child_info['stderr'], template_args) if child_info['stderr'] else None))
        
    
    # Functions to create template arguments and environment values for child processes
    def update_args_env_basis_state(self, template_args, environ, basis_state):
        new_template_args = {'basis_state': basis_state}
        new_env = {self.ENV_BSTATE_ID: str(basis_state.state_id or -1),
                   self.ENV_BSTATE_DATA_REF: self.makepath(self.basis_state_ref_template, new_template_args)}
        template_args.update(new_template_args)
        environ.update(new_env)
        return template_args, environ
    
    def update_args_env_initial_state(self, template_args, environ, initial_state):
        new_template_args = {'initial_state': initial_state}
        new_env = {self.ENV_ISTATE_ID: str(initial_state.state_id or -1),
                   self.ENV_ISTATE_DATA_REF: self.makepath(self.initial_state_ref_template, new_template_args)}
        
        if initial_state.basis_state is not None:
            basis_state = initial_state.basis_state
        else:
            basis_state = self.basis_states[initial_state.basis_state_id]
          
        self.update_args_env_basis_state(new_template_args, new_env, basis_state)
        
        template_args.update(new_template_args)
        environ.update(new_env)
        return template_args, environ
    
    def update_args_env_iter(self, template_args, environ, n_iter):
        environ[self.ENV_CURRENT_ITER] = str(n_iter if n_iter is not None else -1)
        template_args['n_iter'] = int(n_iter) 
        return template_args, n_iter
    
    def update_args_env_segment(self, template_args, environ, segment):
        template_args['segment'] = segment
        
        environ[self.ENV_CURRENT_SEG_INITPOINT] = Segment.initpoint_type_names[segment.initpoint_type]
        
        if segment.initpoint_type == Segment.SEG_INITPOINT_CONTINUES:
            # Could use actual parent object here if the work manager cared to pass that much data
            # to us (we'd need at least the subset of parents for all segments sent in the call to propagate)
            # that may make a good west.cfg option for future crazy extensibility, but for now,
            # just populate the bare minimum
            parent = Segment(n_iter=segment.n_iter-1, seg_id=segment.parent_id)
            parent_template_args = dict(template_args)
            parent_template_args['segment'] = parent
            
            environ[self.ENV_PARENT_SEG_ID] = str(segment.parent_id)            
            environ[self.ENV_PARENT_DATA_REF] = self.makepath(self.segment_ref_template, parent_template_args)
        elif segment.initpoint_type == Segment.SEG_INITPOINT_NEWTRAJ:
            # This segment is initiated from a basis state; WEST_PARENT_SEG_ID and WEST_PARENT_DATA_REF are
            # set to the basis state ID and data ref
            initial_state = self.initial_states[segment.initial_state_id]
            basis_state = self.basis_states[initial_state.basis_state_id]
            
            if self.ENV_BSTATE_ID not in environ:
                self.update_args_env_basis_state(template_args, environ, basis_state)
            if self.ENV_ISTATE_ID not in environ:
                self.update_args_env_initial_state(template_args, environ, initial_state)
            
            assert initial_state.istate_type in (InitialState.ISTATE_TYPE_BASIS, InitialState.ISTATE_TYPE_GENERATED)
            if initial_state.istate_type == InitialState.ISTATE_TYPE_BASIS:
                environ[self.ENV_PARENT_DATA_REF] = environ[self.ENV_BSTATE_DATA_REF]
            else: # initial_state.type == InitialState.ISTATE_TYPE_GENERATED  
                environ[self.ENV_PARENT_DATA_REF] = environ[self.ENV_ISTATE_DATA_REF]
            
        environ[self.ENV_CURRENT_SEG_ID] = str(segment.seg_id or -1)
        environ[self.ENV_CURRENT_SEG_DATA_REF] = self.makepath(self.segment_ref_template, template_args)
        return template_args, environ
    
    def template_args_for_segment(self, segment):
        template_args, environ = {}, {}
        self.update_args_env_iter(template_args, environ, segment.n_iter)
        self.update_args_env_segment(template_args, environ, segment)
        return template_args
    
    def exec_for_segment(self, child_info, segment, addtl_env = None):
        '''Execute a child process with environment and template expansion from the given
        segment.'''
        template_args, environ = {}, {}
        self.update_args_env_iter(template_args, environ, segment.n_iter)
        self.update_args_env_segment(template_args, environ, segment)        
        environ.update(addtl_env or {})
        self.prepare_file_system(child_info, segment, environ)
        child_info['cwd'] = environ['WEST_CURRENT_SEG_DATA_REF']
        return self.exec_child_from_child_info(child_info, template_args, environ)

    def prepare_file_system(self, child_info, segment, environ):
        try:
            # If the filesystem is properly clean.
            os.makedirs(environ['WEST_CURRENT_SEG_DATA_REF'])
        except:
            # If the filesystem is NOT properly clean.
            shutil.rmtree(environ['WEST_CURRENT_SEG_DATA_REF'])
            os.makedirs(environ['WEST_CURRENT_SEG_DATA_REF'])
        if self.data_info['restart']['enabled']:
            restart_output(tarball='{}/'.format(environ['WEST_CURRENT_SEG_DATA_REF']), segment=segment)

    def cleanup_file_system(self, child_info, segment, environ):
        shutil.rmtree(environ['WEST_CURRENT_SEG_DATA_REF'])
            
    def exec_for_iteration(self, child_info, n_iter, addtl_env = None):
        '''Execute a child process with environment and template expansion from the given
        iteration number.'''
        template_args, environ = {}, {}
        self.update_args_env_iter(template_args, environ, n_iter)
        environ.update(addtl_env or {})
        return self.exec_child_from_child_info(child_info, template_args, environ)

    def exec_for_basis_state(self, child_info, basis_state, addtl_env = None):
        '''Execute a child process with environment and template expansion from the
        given basis state'''
        template_args, environ = {}, {}
        self.update_args_env_basis_state(template_args, environ, basis_state)
        environ.update(addtl_env or {})
        return self.exec_child_from_child_info(child_info, template_args, environ)
        
    def exec_for_initial_state(self, child_info, initial_state,  addtl_env = None):
        '''Execute a child process with environment and template expansion from the given
        initial state.'''
        template_args, environ = {}, {}
        self.update_args_env_initial_state(template_args, environ, initial_state)
        environ.update(addtl_env or {})
        return self.exec_child_from_child_info(child_info, template_args, environ)

    # Specific functions required by the WEST framework
    def get_pcoord(self, state):
        '''Get the progress coordinate of the given basis or initial state.'''
        
        template_args, environ = {}, {}
        #state.error = []
        #state.status = None
        
        if isinstance(state, BasisState):
            execfn = self.exec_for_basis_state
            self.update_args_env_basis_state(template_args, environ, state)
            struct_ref = environ[self.ENV_BSTATE_DATA_REF]
        elif isinstance(state, InitialState):
            execfn = self.exec_for_initial_state
            self.update_args_env_initial_state(template_args, environ, state)
            struct_ref = environ[self.ENV_ISTATE_DATA_REF]
        else:
            raise TypeError('state must be a BasisState or InitialState')
        
        child_info = self.exe_info.get('get_pcoord')
        pfd, prfname = tempfile.mkstemp()
        os.close(pfd)
        cfd, crfname = tempfile.mkstemp()
        os.close(cfd)
        erfname = tempfile.mkdtemp()
        
        addtl_env = {self.ENV_PCOORD_RETURN:     prfname,
                     self.ENV_RESTART_RETURN:    erfname,
                     self.ENV_TRAJECTORY_RETURN: crfname,
                     self.ENV_STRUCT_DATA_REF:   struct_ref}

        try:
            #rc, rusage = execfn(child_info, state, addtl_env)
            results = execfn(child_info, state, addtl_env)
            rc, rusage, err = results[1]
            state.err = err
            if rc != 0:
                log.error('get_pcoord executable {!r} returned {}'.format(child_info['executable'], rc))
                
            # Why do we load the pcoord data last?  Because we may well want the restart/trajectory information.
            # And indeed, we should send in the temp directory for the restart information to the pcoord loader
            # so that it has the topology information, if necessary, and the current file thing.
            if self.data_info['trajectory']['enabled']:
                cloader = self.data_info['trajectory']['loader']
                cloader('trajectory', crfname, state, single_point = True)
            if self.data_info['restart']['enabled']:
                eloader = self.data_info['restart']['loader']
                eloader('restart', erfname, state, single_point = True)
            else:
                state.data['trajectories/restart'] = None
            ploader = self.data_info['pcoord']['loader']
            porig = state.pcoord
            try:
                ploader('pcoord', prfname, state, single_point = True, trajectory=crfname, restart=erfname)
                state.status = Segment.SEG_STATUS_COMPLETE
            except Exception as e:
                a = traceback.format_exc()
                a = "\n        ".join(a.splitlines()[:])
                #error.report_general_error_once(error.EMPTY_TRAJECTORY, segment=segment)
                state.error.append(error.report_segment_error(error.EMPTY_TRAJECTORY, segment=state, filename=prfname, dataset='pcoord', e=e, loader=ploader, traceback=a))
                state.status = Segment.SEG_STATUS_FAILED
            check_pcoord(state, original_pcoord=porig, single_point=True, executable=child_info['executable'], logfile=child_info['stdout'])
        finally:
            try:
                os.unlink(prfname)
            except Exception as e:
                log.warning('could not delete progress coordinate return file {!r}: {}'.format(prfname, e))
            if self.data_info['trajectory']['enabled']:
                try:
                    os.unlink(crfname)
                except Exception as e:
                    log.warning('could not delete trajectory coordinate return file {!r}: {}'.format(crfname, e))
            if self.data_info['restart']['enabled']:
                try:
                    shutil.rmtree(erfname)
                except Exception as e:
                    log.warning('could not delete restart return directory {!r}: {}'.format(erfname, e))
                
    def gen_istate(self, basis_state, initial_state):
        '''Generate a new initial state from the given basis state.'''
        child_info = self.exe_info.get('gen_istate')
        #rc, rusage = self.exec_for_initial_state(child_info, initial_state)
        results = self.exec_for_initial_state(child_info, initial_state)
        rc, rusage, err = results[1]
        if rc != 0:
            log.error('gen_istate executable {!r} returned {}'.format(child_info['executable'], rc))
            initial_state.istate_status = InitialState.ISTATE_STATUS_FAILED
            return            
    
        # Determine and load the progress coordinate value for this state
        try:
            self.get_pcoord(initial_state)
        except Exception as e:
            #log.exception('could not get progress coordinate for initial state {!r}'.format(initial_state))
            a = traceback.format_exc()
            #a = a.split('\n')
            a = "\n        ".join(a.splitlines()[:])
            #initial_state.error = []
            initial_state.error.append(error.report_segment_error(error.ISTATE_ERROR, segment=initial_state, filename='', dataset='pcoord', e=e, loader=self.get_pcoord, traceback=a))
            initial_state.istate_status = InitialState.ISTATE_STATUS_FAILED
            raise
        else:
            initial_state.istate_status = InitialState.ISTATE_STATUS_PREPARED
                        
    def prepare_iteration(self, n_iter, segments):
        child_info = self.exe_info.get('pre_iteration')
        if child_info and child_info['enabled']:
            try:
                #rc, rusage = self.exec_for_iteration(child_info, n_iter)
                results = self.exec_for_iteration(child_info, n_iter)
                rc, rusage = results[1]
            except OSError as e:
                log.warning('could not execute pre-iteration program {!r}: {}'.format(child_info['executable'], e))
            else:
                if rc != 0:
                    log.warning('pre-iteration executable {!r} returned {}'.format(child_info['executable'], rc))
        
    def finalize_iteration(self, n_iter, segments):
        child_info = self.exe_info.get('post_iteration')
        if child_info and child_info['enabled']:
            try:
                #rc, rusage = self.exec_for_iteration(child_info, n_iter)
                results = self.exec_for_iteration(child_info, n_iter)
                rc, rusage, err = results[1]
            except OSError as e:
                log.warning('could not execute post-iteration program {!r}: {}'.format(child_info['executable'], e))
            else:
                if rc != 0:
                    log.warning('post-iteration executable {!r} returned {}'.format(child_info['executable'], rc))
        
                
    def propagate(self, segments):
        child_info = self.exe_info['propagator']
        
        for segment in segments:
            #segment.error = []
            starttime = time.time()

            addtl_env = {}
            
            return_files = {}
            del_return_files = {}
            
            #for dataset in sorted(self.data_info, reverse=True):
            # We want to load the progress coordinate LAST, and it's set into the ordereddict first, so.
            #for dataset in reversed(self.data_info):
            for dataset in self.data_info:
                if not self.data_info[dataset].get('enabled',False):
                    continue
 
                return_template = self.data_info[dataset].get('filename')
                if return_template:
                    return_files[dataset] = self.makepath(return_template, self.template_args_for_segment(segment))
                    del_return_files[dataset] = False
                elif dataset == 'restart':
                    rfname = tempfile.mkdtemp()
                    return_files[dataset] = rfname
                    del_return_files[dataset] = True
                else: 
                    (fd, rfname) = tempfile.mkstemp()
                    os.close(fd)
                    return_files[dataset] = rfname
                    del_return_files[dataset] = True

                addtl_env['WEST_{}_RETURN'.format(dataset.upper())] = return_files[dataset]
                # This is where it all goes down.
                                        
            # We're going to want to output the extra coordinates used for stuff...

            # Spawn propagator and wait for its completion
            #used_environ, rc, rusage = self.exec_for_segment(child_info, segment, addtl_env) 
            results = self.exec_for_segment(child_info, segment, addtl_env) 
            rc, rusage, err = results[1]
            segment.err = err
            run_environ = results[0]

            if self.cleanup == True:
                self.cleanup_file_system(child_info, segment, run_environ)
            
            if rc == 0:
                segment.status = Segment.SEG_STATUS_COMPLETE
            elif rc < 0:
                #log.error('child process for segment %d exited on signal %d (%s)' % (segment.seg_id, -rc, SIGNAL_NAMES[-rc]))
                segment.error.append(error.report_segment_error(error.RUNSEG_SIGNAL_ERROR, segment=segment, err=err, rc=-rc))
                segment.status = Segment.SEG_STATUS_FAILED
                continue
            else:
                #log.error('child process for segment %d exited with code %d' % (segment.seg_id, rc))
                segment.error.append(error.report_segment_error(error.RUNSEG_GENERAL_ERROR, segment=segment, err=err, rc=rc))
                segment.status = Segment.SEG_STATUS_FAILED
                continue
            
            # Extract data and store on segment for recording in the master thread/process/node
            # We want to load the pcoord last, as we may wish to directly manipulate trajectories.
            # Actually, I take it back.  We want to load the pcoord first such that we may calculate properties
            # on the trajectory while it still exists in the filesystem.
            #for dataset in self.data_info:
            for dataset in reversed(self.data_info):
                # pcoord is always enabled (see __init__)
                if not self.data_info[dataset].get('enabled',False):
                    continue
                
                filename = return_files[dataset]
                loader = self.data_info[dataset]['loader']
                try:
                    segment.file_type = self.trajectory_types
                except:
                    pass
                try:
                    if dataset == 'pcoord':
                        # Yes, I'm considering changing the default behavior.  It's faster to just supply the files directly on disk during propagation,
                        # rather than re-creating temp files just to have it work for a custom pcoord load function.  Really, we just need to make sure
                        # that the pcoord loaders accept *kwargs; nothing else should be necessary.
                        #try:
                        porig = segment.pcoord
                        if self.data_info['restart']['enabled']:
                            loader(dataset, filename, segment, single_point=False, trajectory=return_files['trajectory'], restart=return_files['restart'])
                        else:
                            loader(dataset, filename, segment, single_point=False, trajectory=return_files['trajectory'], restart=None)
                        check_pcoord(segment, original_pcoord=porig, single_point=False, executable=child_info['executable'], logfile=child_info['stdout'])
                        #except:
                            # Compatibility for older calls.  If this call doesn't work, the normal error handling should sort it.
                        #    porig = segment.pcoord
                        #    loader(dataset, filename, segment, single_point=False)
                        #    check_pcoord(segment, original_pcoord=porig, single_point=False, executable=child_info['executable'], logfile=child_info['stdout'])
                    else:
                        loader(dataset, filename, segment, single_point=False)
                except Exception as e:
                    #print(log.exception(e))
                    a = traceback.format_exc()
                    #a = a.split('\n')
                    a = "\n        ".join(a.splitlines()[:])
                    #print(e, dataset)
                    #if dataset != 'pcoord':
                    #    error.report_segment_error(error.RUNSEG_TMP_ERROR, segment=segment, filename=filename, dataset=dataset, e=e)

                    # We catch this if the error hasn't already been handled.
                    if e.__class__ != error.ErrorHandled:
                        segment.error.append(error.report_segment_error(error.RUNSEG_TMP_ERROR, segment=segment, filename=filename, dataset=dataset, e=e, loader=loader, traceback=a))
                    #else:
                    #    error.report_segment_error(error.EMPTY_PCOORD_ERROR, segment=segment, filename=filename, dataset=dataset, e=e)
                    #log.error('could not read {} from {!r}: {!r}'.format(dataset, filename, e))
                    segment.status = Segment.SEG_STATUS_FAILED 
                    break

            # Why are we deleting the dataset AFTER we load it?  We want to expose the trajectory and restart information to the
            # pcoord loader, if applicable.
            #for dataset in reversed(self.data_info):
            for dataset in self.data_info:
                if not self.data_info[dataset].get('enabled',False):
                    continue
                filename = return_files[dataset]
                if del_return_files[dataset]:
                    if dataset == 'restart':
                        try:
                            shutil.rmtree(filename)
                        except Exception as e:
                            log.warning('could not delete {} file {!r}: {!r}'.format(dataset, filename, e))
                        else:
                            log.debug('deleted {} directory {!r}'.format(dataset, filename))    
                    else:
                        try:
                            os.unlink(filename)
                        except Exception as e:
                            log.warning('could not delete {} file {!r}: {!r}'.format(dataset, filename, e))
                        else:
                            log.debug('deleted {} file {!r}'.format(dataset, filename))    
            if segment.status == Segment.SEG_STATUS_FAILED:
                continue
                                        
            # Record timing info
            segment.walltime = time.time() - starttime
            segment.cputime = rusage.ru_utime
        # Clean up the file system.
        return segments
