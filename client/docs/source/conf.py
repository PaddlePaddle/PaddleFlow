# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
import sys
from pathlib import Path
paddleflow_home = Path(__file__).parent.parent.parent
pipeline_home = paddleflow_home / "paddleflow"
sys.path.insert(0, str(paddleflow_home))
sys.path.insert(0, str(pipeline_home))


# -- Project information -----------------------------------------------------

project = 'PaddleFlow Pipeline'
copyright = '2022, PaddleFlow Pipeline Group'
author = 'PaddleFlow Pipeline Group'

# The full version, including alpha/beta/rc tags
release = '1.4.1'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.napoleon"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["**/tests/**"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

autoclass_content = 'both'


#------------- 处理 docstring
def remove_module_docstring(app, what, name, obj, options, lines):
    """ remove docstring of module
    """
    if what == "module":
        del lines[:]


def remove_create_instance_string(app, what, name, obj, options, lines):
    """ remove 'create new instances' in docstring
    """
    for line in lines:
        if "create a new instance" in line or \
                line.startswith("创建一个新的") or \
                "Create a new instance" in line:

            lines.remove(line)


def setup(app):
    """ setup
    """
    app.connect("autodoc-process-docstring", remove_module_docstring)
    app.connect("autodoc-process-docstring", remove_create_instance_string)
