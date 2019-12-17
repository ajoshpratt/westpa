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

WESTPA is free software, licensed under the terms of the MIT License. See the file ``LICENSE`` for more information.

.. _survey: https://docs.google.com/forms/d/e/1FAIpQLSfWaB2aryInU06cXrCyAFmhD_gPibgOfFk-dspLEsXuS9-RGQ/viewform
.. _page: https://westpa.github.io/westpa/overview.html

--------------------------------
Installing WESTPA
--------------------------------

First, install the `Anaconda Python distribution`_. Then, make sure you are able to activate conda environments (this is usually taken care of by the Anaconda installer).

WESTPA can then be installed through conda in a dedicated environment with the following.

``conda create -n westpa -c conda-forge westpa``
  
WESTPA will be ready to use after activation with the following command.

``. $(dirname $(dirname `which python2.7`))/$conda_env/westpa-2017.10/westpa.sh``
  
Feel free to install any other conda packages alongside WESTPA in your environment. AmberTools, GROMACS and OpenMM all
provide conda installations of their MD packages. An example command to create an environment containing WESTPA and AmberTools is given below.

``conda create -n westpa -c conda-forge -c ambermd westpa ambertools``
    
.. _`Anaconda Python distribution`: https://www.anaconda.com/distribution/ 

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


