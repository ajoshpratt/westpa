===============
WESTPA 1.0
===============


--------
Overview
--------

WESTPA is a package for constructing and running stochastic simulations using the "weighted ensemble" approach 
of Huber and Kim (1996). For use of WESTPA please cite the following:

Zwier, M.C., Adelman, J.L., Kaus, J.W., Pratt, A.J., Wong, K.F., Rego, N.B., Suarez, E., Lettieri, S.,
Wang, D. W., Grabe, M., Zuckerman, D. M., and Chong, L. T. "WESTPA: An Interoperable, Highly 
Scalable Software Package For Weighted Ensemble Simulation and Analysis," J. Chem. Theory Comput., 11: 800−809 (2015). 

See this page_ for an overview of weighted ensemble simulation.

To help us fund development and improve WESTPA please fill out a one-minute survey_ and consider 
contributing documentation or code to the WESTPA community.

WESTPA is free software, licensed under the terms of the GNU General Public
License, Version 3. See the file ``COPYING`` for more information.

.. _survey: https://docs.google.com/forms/d/e/1FAIpQLSfWaB2aryInU06cXrCyAFmhD_gPibgOfFk-dspLEsXuS9-RGQ/viewform
.. _page: https://westpa.github.io/westpa/overview.html

------------
Requirements
------------

WESTPA is written in Python and requires version 2.7. WESTPA further requires
a large number of scientific software libraries for Python and other
languages. The simplest way to meet these requirements is to download the
Anaconda Python distribution from www.continuum.io (free for all users).

WESTPA currently runs on Unix-like operating systems, including Linux and
Mac OS X. It is developed and tested on x86_64 machines running Linux.


------------
Installation
------------

After obtaining a copy of the code (see https://westpa.github.io/westpa for details), run
``setup.sh`` in the ``westpa`` directory. If the version of Python you will
be using to run the code is not first on your $PATH, then set the environment
variable WEST_PYTHON to the Python interpreter you want to use. For example::

    cd westpa
    export WEST_PYTHON=/opt/anaconda/bin/python2.7
    ./setup.sh

A ``westpa.sh`` script is created during installation, and will set the following
environment variables::

    WEST_ROOT
    WEST_BIN
    WEST_PYTHON

For installation on a cluster with modules, system administrators must set these
variables to the appropriate values within the module file.

---------------
Getting started
---------------

To define environment variables post-installation, simply source the 
``westpa.sh`` script in the ``westpa`` directory from the command line
or your setup scripts.

High-level tutorials of how to use the WESTPA software can be found here_.
Further, all WESTPA command-line tools (located in ``westpa/bin``) provide detailed help when
given the -h/--help option.

Finally, while WESTPA is a powerful tool that enables expert simulators to access much longer 
timescales than is practical with standard simulations, there can be a steep learning curve to 
figuring out how to effectively run the simulations on your computing resource of choice. 
For serious users who have completed WESTPA tutorials and are ready for production simulations 
of their system, we invite you to contact Lillian Chong (ltchong AT pitt DOT edu) about visiting her lab 
and/or setting up video conferencing calls to help get your simulations off the ground.

.. _here: https://github.com/westpa/westpa/wiki/WESTPA-Tutorials


------------
Getting help
------------

A mailing list for WESTPA is available, at which one can ask questions (or see
if a question one has was previously addressed). This is the preferred means
for obtaining help and support. See http://groups.google.com/group/westpa-users
to sign up or search archived messages.


