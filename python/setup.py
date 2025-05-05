#!/usr/bin/env python3
#
# This should be invoked from the top-level Gradle build, so that the
# environment is correctly set up:
#  gradlew develop  -- Local testing.
#  gradlew wheel    -- Build the distribution wheel.
#

from setuptools import setup, Extension

import numpy
import os

# Package settings.
name = 'pjrmi'

# Parameters which are set by the Gradle build enironment.
version     = os.environ.get('PJRMI_VERSION')
cpp_include = os.environ.get('INCLUDE_PATH')
java_home   = os.environ.get('JAVA_HOME')

# For the libjvm.so library.
java_lib      = os.path.join(java_home, 'lib', 'server')
java_includes = [os.path.join(java_home, 'include'         ),
                 os.path.join(java_home, 'include', 'linux')]

# Check the Java configuration. The path to the Java runtime can vary from
# system to system, ensure that it was correctly set up and we have what we
# need from it.
if not os.path.exists(java_home):
    raise ValueError(f'JAVA_HOME "{java_home}" does not exist')
if not os.path.exists(java_lib):
    raise ValueError(f'Java library directory {java_lib}" does not exist')
for java_include in java_includes:
    if not os.path.exists(java_include):
        raise ValueError(f'Java include directory {java_include}" does not exist')

# Package the PJRmi shared libraries required by the extension and the Java JAR.
extra_libs_dir = "pjrmi/lib"

# Extension arguments.
#
# The `$ORIGIN/lib` in the runpath is a special linker directive, understood by
# `ld`, which will allow the extension to runtime link against the core PJRmi
# library.
include_dirs         = [cpp_include, numpy.get_include()] + java_includes
library_dirs         = [extra_libs_dir, java_lib]
runtime_library_dirs = [java_lib, "$ORIGIN/lib"]
libraries            = [name, 'jvm']

# And invoke!
setup(
    name        =name,
    version     =version,
    description ='PJRmi, RMI between Python and Java',
    url         ="https://github.com/deshaw/pjrmi",
    packages    =['pjrmi'],
    package_dir ={'pjrmi': 'pjrmi'},
    package_data={'pjrmi': ["lib/**"]},
    ext_modules =[Extension(name + '.extension',
                            ['extension/extension.cpp'],
                            include_dirs        =include_dirs,
                            library_dirs        =library_dirs,
                            runtime_library_dirs=runtime_library_dirs,
                            libraries           =libraries)],
)
