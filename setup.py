from distutils.core import setup, Extension
#from Cython.Build import cythonize
import sys, string, os
import shutil

args = {"name": "webstorage",
        "author": "Arthur Messner",
        "author_email": "arthur.messner@gmail.com",
        "description": "WebStorage Archiving System",
        "url" : "https://github.com/gunny26/webstorage",
        "long_description": __doc__,
        "platforms": ["any", ],
        "license": "LGPLv2",
        "packages": ["webstorageClient", "webstorageServer"],
        "scripts": ["bin/wstar.py", "bin/wsclient.py"],
        # Make packages in root dir appear in pywbem module
        "package_dir": {
            "webstorageClient": "client",
            "webstorageServer" : "server"
            },
        # Make extensions in root dir appear in pywbem module
        #"ext_package": "webstorage",
        # "ext_modules" : cythonize("*.pyx"),
        "requires" : ["requests", ],
        "version" : "2.0.2",
        }
setup(**args)
