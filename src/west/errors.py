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

from __future__ import division, print_function

import logging, argparse, traceback, os
log = logging.getLogger('errors.py')
import westpa
import sys
#sys.tracebacklimit=0

class WESTErrorReporting:
    """
    General class for handling more user-friendly error messages.
    Programs should import this and create an instance of it; then,
    instead of raising specific errors through the Python interface, they
    should just call "report error" and format the arguments accordingly.
    This way, the users see far more specific information about the actual
    error.

    """

    def __init__(self, cp=''):
        self.pstatus = westpa.rc.pstatus
        self.shell_variables = None
        self.script = None
        log.debug('initializing error handling')
        self.config = westpa.rc.config
        self.system = westpa.rc.get_system_driver()
        try:
            self.report_all_errors = self.config['west']['error']['report_all_errors']
        except:
            self.report_all_errors = False
        try:
            self.error_lines = self.config['west']['error']['stderr_lines']
        except:
            self.error_lines = 10
        self.reported_errors = {}
        # Calling program.  Useful for saying who threw the exception.
        self.cp = cp

        # westpa.rc.pstatus('interrupted; shutting down')

        wiki = "https://chong.chem.pitt.edu/wewiki/WESTPA_Error_Handling#{id}"

        # We're going to TRY and load up the propagator information.  However, this won't necessarily work.
        try:
            # There's probably a more 'correct' way to do this via the _rc.  I should check into that,
            # as this clearly relies on some hard coded values, which is bad.
            executable = os.path.expandvars(self.config['west']['executable']['propagator']['executable'])
            logfile = self.config['west']['executable']['propagator']['stdout']
            # Actually, it's fine.  We treat the built-in executable prop with some care, anyway.
        except:
            executable = '-NA-'
            logfile = 'stdout'

        rcfile = self.config['args']['rcfile']
        pcoord_len = self.system.pcoord_len
        pcoord_ndim = self.system.pcoord_ndim
        self.llinebreak = "-"*64
        self.linebreak = "-"*42
        self.format_kwargs = { 'executable': executable, 'rcfile': rcfile, 'pcoord_ndim': pcoord_ndim, 'pcoord_len': pcoord_len, 'logfile': logfile,
                'wiki': wiki, 'linebreak': self.linebreak, 'cp': cp, 'llinebreak': self.llinebreak , 'error_lines': self.error_lines }

        self.SEG_ERROR            = """
        {llinebreak}{linebreak}
        ERROR # {id} ON Iteration: {segment.n_iter}, {segment_type}: {segment.seg_id}"""

        self.ITER_ERROR = """
        {llinebreak}{linebreak}
        ERROR # {id} ON Iteration: {iteration}"""

        self.RUNSEG_GENERAL_ERROR = { 'msg': """
        A general error has been caught from the {executable} propagator.
        You should check the indicated log file for more specific errors,
        or see below.

        FILES TO CHECK

        {logfile}
        {executable}


        LAST {error_lines} LINES OF STDERR
        {linebreak}
        {err}
        {linebreak}
        """,
        'id': 'E0' }

        self.RUNSEG_SHAPE_ERROR = { 'msg': """
        The shape of your progress coordinate return value is {shape},
        which is different from what is specified in your {rcfile}: ({pcoord_len}, {pcoord_ndim}).  

        FILES TO CHECK

        {executable}
        {rcfile}
        Your dynamics engine configuration file.

        They should all agree on number of values/timepoints/progress coordinate values
        that you are returning.

        See {logfile}
        """,
        'id': 'E1' }

        self.RUNSEG_TMP_ERROR = { 'msg': """
        Could not read the {dataset} return value from {filename} for segment {segment.seg_id} in iteration {segment.n_iter}.

        POSSIBLE REASONS

        {executable} is not returning anything into the {dataset} return.
            - This could be the result of a failed run, or
            - {executable} is not returning data into the {dataset} return
              (typically, cat/paste into the WEST_DATASET_RETURN variable)
        {filename} is not writable.
            - The space {filename} exists on could be full.  Try cleaning it.
        {loader.__module__}.{loader.func_name} is throwing an error.

        FILES TO CHECK

        {logfile}
        {executable}
        {rcfile} - did you want to return this dataset?

        Specific exception:

        {linebreak}
        {loader.__module__}.{loader.func_name}:
        {e}

        {traceback}
        {linebreak}
        """,
        'id': 'E2' }

        self.RUNSEG_AUX_ERROR = { 'msg': """
        Your auxiliary data return is empty.  If the simulation fails,
        ensure that you're returning the auxiliary data properly and
        that the {executable} propagator is error-free.  If the simulation
        does not fail, disable the return option in {rcfile}.
        Check the indicated log file for more specific errors:

        FILES TO CHECK
        {linebreak}
        {logfile}

        LAST 10 LINES OF STDERR
        {linebreak}
        {err}
        {linebreak}

        """,
        'id': 'E3' }

        self.RUNSEG_PROP_ERROR = { 'msg': """
        Propagation has failed for {failed_segments} segments:
        {linebreak}
        {failed_ids}
        {linebreak}

        Check the corresponding log files for each ID.
        """,
        'id': 'E4' }

        self.EMPTY_PCOORD_ERROR = { 'msg': """
        The pcoord dataset on seg_id {segment.seg_id} is empty.  Check your 
        {executable} propagator, the indicated log file, or any custom pcoord loader function
        for more information.

        FILES/FUNCTIONS TO CHECK

        {logfile}
        {executable}
        Any custom pcoord loader, if using.

        LAST {error_lines} LINES OF STDERR
        {linebreak}
        {err}
        {linebreak}
        """,
        'id': 'E5' }

        self.PCOORD_LOADER_ERROR = { 'msg': """
        The pcoord dataset on seg_id {segment.seg_id} is unable to be loaded via
        numpy.loadtxt().  Check the indicated log file for more information, and 
        the {executable} propagator to ensure that the data you are returning is 
        suitable for numpy.loadtxt().

        FILES/FUNCTIONS TO CHECK

        {logfile}
        {executable}
        Any custom pcoord loader, if using.

        LAST {error_lines} LINES OF STDERR
        {linebreak}
        {err}
        {linebreak}
        """,
        'id': 'E6' }

        self.EMPTY_TRAJECTORY = { 'msg': """
        The trajectory return for seg_id {segment.seg_id} is empty.  If you're not
        storing trajectory data for this WESTPA run, please disable the trajectory
        return in {rcfile}.  Otherwise, ensure that $WEST_TRAJECTORY_RETURN is not empty.

        FILES/FUNCTIONS TO CHECK

        {logfile}
        {executable}
        {rcfile}

        """,
        'id': 'E7' }

        self.EMPTY_RESTART = { 'msg': """
        The restart return for seg_id {segment.seg_id} is empty.  If you're not
        storing restart data for this WESTPA run, please disable the restart
        return in {rcfile}.  Otherwise, ensure that $WEST_RESTART_RETURN is not empty.

        FILES/FUNCTIONS TO CHECK

        {logfile}
        {executable}
        {rcfile}

        """,
        'id': 'E8' }

        self.LARGE_RESTART = { 'msg': """
        The restart return for seg_id {segment.seg_id} is {size}.  This is likely to
        result in RAM-intensive WESTPA runs.  Check your propagator to ensure that
        you're sending in the minimum amount of data.  This is not fatal.
        FILES/FUNCTIONS TO CHECK

        {executable}

        """,
        'id': 'E9' }

        self.ISTATE_ERROR = { 'msg': """
        ISTATE GENERATION FAILURE: Could not read the {dataset} return value istate {segment.seg_id} in iteration {segment.n_iter}.

        This is typically due to a failure to return the progress coordinate for istates/bstates.  Check the appropriate function.

        Specific exception:

        {linebreak}
        {loader.__module__}.{loader.func_name}:
        {e}

        {traceback}
        {linebreak}
        """,
        'id': 'E10' }

        self.WRUN_INTERRUPTED = { 'msg': """
        INTERRUPTION

        An interruption has been sent to {cp}.
        This has either been done manually (such as the break command or the killing of a queue script),
        or by the local sysadmin.
        """,
        'id': 'W5' }

        self.RUNSEG_EMPTY_VARIABLES = { 'msg': """
        NOTICE

        Empty variables exist in your {executable}.  This could be a problem.

        {linebreak}
        {empties}
        {linebreak}

        """,
        'id': 'E99' }

        self.REPORT_ONCE = """
        NOTICE

        The configuration has been set such that each error type is caught only once; all other
        segments which report the same error will have their output suppressed.  This can be disabled.
        """

        self.SEE_WIKI = """
        Check the wiki for more information
        https://chong.chem.pitt.edu/wewiki/WESTPA_Error_Handling#ERROR_{id}

        {llinebreak}{linebreak}
        """ 


    def report_segment_error(self, error, segment, **kwargs):
        #sys.tracebacklimit=0
        # We'll want to pass in the segment object, actually.  But we can't call that from here...
        # ... but, this should still work, for the moment.
        self.format_kwargs.update(kwargs)
        # Pull in the ID.
        self.format_kwargs.update(error)
        self.format_kwargs.update({'segment': segment})
        # Testing for istate/bstates, as they'll get passed in the same way.
        try:
            test = segment.n_iter
            self.format_kwargs.update({'segment_type': 'Segment' })
        except:
            segment.n_iter = 0
            segment.seg_id = segment.state_id
            self.format_kwargs.update({'segment_type': 'istate/bstate' })
        try:
            # These ones need modifying, as the segment.niter doesn't work with format, directly.
            self.format_kwargs['logfile'] = os.path.expandvars(self.format_kwargs['logfile'].format(segment=segment))
        except:
            pass

        # How can we enable it such that we report one 'type' of error only once?
        # Often, we repeat many errors and it's a pain.  Sometimes, this is useful information,
        # but most of the time it's just indicative of a general problem.
        # In the typical python fashion, we ask forgiveness, not permission.
        if self.report_all_errors == False:
            try:
                if self.reported_errors[self.REPORT_ONCE] == False:
                    self.pstatus(self.REPORT_ONCE.format(**self.format_kwargs))
                    self.reported_errors[self.REPORT_ONCE] = True
            except:
                self.pstatus(self.REPORT_ONCE.format(**self.format_kwargs))
                self.reported_errors[self.REPORT_ONCE] = True

        try:
            if self.reported_errors[error['msg']] == False:
                self.pstatus(self.SEG_ERROR.format(**self.format_kwargs))
                self.pstatus(error['msg'].format(**self.format_kwargs))
                self.pstatus(self.SEE_WIKI.format(**self.format_kwargs))
                if self.report_all_errors == False:
                    self.reported_errors[error['msg']] = True
        except:
            self.pstatus(self.SEG_ERROR.format(**self.format_kwargs))
            self.pstatus(error['msg'].format(**self.format_kwargs))
            self.pstatus(self.SEE_WIKI.format(**self.format_kwargs))
            if self.report_all_errors == False:
                self.reported_errors[error['msg']] = True

    def report_error(self, error, **kwargs):
        #sys.tracebacklimit=0
        self.format_kwargs.update(kwargs)
        # Pull in the ID.
        self.format_kwargs.update(error)
        self.pstatus(self.ITER_ERROR.format(**self.format_kwargs))
        self.pstatus(error['msg'].format(**self.format_kwargs))
        self.pstatus(self.SEE_WIKI.format(**self.format_kwargs))

    def scan_shell_variables(self, script):
        if self.shell_variables == None or self.script != script:
            # Let's not run this more than once, shall we?
            self.script = script
            self.shell_variables = []
            import re
            with open(script, 'r') as runseg:
                for line in runseg:
                    # Find the variables!
                    var_group = re.findall('\$\w+', line)
                    if len(var_group) > 0:
                        self.shell_variables += var_group
            self.shell_variables = [self.remove_from_string(s, '$') for s in self.shell_variables]

    def scan_shell_empty_variables(self, out):
        import re
        empties = []
        statedv = []
        # Let's place all these in the empty variables...
        # ... but also sort the others into the 'good' variable
        # section.
        for line in out.splitlines():
            empty = re.findall('\w+=$', line)
            # This should match everything BUT a carriage return, newline, or space.
            # Empties just sort of works, I think.
            filled = re.findall('^\w+=(?!\&$| $|\n$).*', line)
            empties += empty
            statedv += filled
        return [self.remove_from_string(s, '=') for s in empties], statedv

    def remove_from_string(self, s, r):
        return s.replace(r, '')

    def does_not_exist_in_list(self, l1, l2):
        # We want to see if any string in l1 is a part of string
        # in l2.
        # We'll return all values that are in the second list.
        rl = []
        for s1 in l1:
            exists = False
            for s2 in l2:
                if s1 in s2:
                    exists = True
            if exists == False:
                rl.append(s1)
        return rl

    def scan_for_shell(self, executable, out, stderr):
        self.scan_shell_variables(executable)
        #print(self.shell_variables)
        # Then, scan the output for all filled and empty variables.
        empties, filled = self.scan_shell_empty_variables(out)
        # Let's get the ones that are called, but not specifically in the env (that is, never even stated).
        empties += self.does_not_exist_in_list(self.shell_variables, filled)
        # Get rid of duplicates.
        empties = list(set(empties))
        # Okay, now we want to check to see if any of the called variables exist within
        if len(empties) > 0:
                stderr.write('\n\n\n' + self.linebreak + ' EMPTY_VARIABLES ' + self.linebreak + '\n\n\n')
                #for empty in empties:
                stderr.write("\n".join(empties))
                self.report_general_error_once(self.RUNSEG_EMPTY_VARIABLES, empties="\n        ".join(empties))

    def report_general_error_once(self, error, **kwargs):
        #sys.tracebacklimit=0
        # This is a function that respects the 'run only once' setting,
        # but doesn't require extensive iteration.  It's useful for printing a
        # warning a during simulation.

        self.format_kwargs.update(kwargs)
        # Pull in the ID.
        self.format_kwargs.update(error)
        # How can we enable it such that we report one 'type' of error only once?
        # Often, we repeat many errors and it's a pain.  Sometimes, this is useful information,
        # but most of the time it's just indicative of a general problem.
        # In the typical python fashion, we ask forgiveness, not permission.
        if self.report_all_errors == False:
            try:
                if self.reported_errors[self.REPORT_ONCE] == False:
                    self.pstatus(self.REPORT_ONCE.format(**self.format_kwargs))
                    self.reported_errors[self.REPORT_ONCE] = True
            except:
                self.pstatus(self.REPORT_ONCE.format(**self.format_kwargs))
                self.reported_errors[self.REPORT_ONCE] = True

        try:
            if self.reported_errors[error['msg']] == False:
                self.pstatus(error['msg'].format(**self.format_kwargs))
                self.pstatus(self.SEE_WIKI.format(**self.format_kwargs))
                if self.report_all_errors == False:
                    self.reported_errors[error['msg']] = True
        except:
            self.pstatus(error['msg'].format(**self.format_kwargs))
            self.pstatus(self.SEE_WIKI.format(**self.format_kwargs))
            if self.report_all_errors == False:
                self.reported_errors[error['msg']] = True

    class ErrorHandled(Exception):
        pass

    def raise_exception(self):
        raise self.ErrorHandled('Error reported from {}'.format(self.cp))

    def format_stderr(self, err):
        return  "\n        ".join(err.splitlines()[(-1*self.error_lines):]) 


