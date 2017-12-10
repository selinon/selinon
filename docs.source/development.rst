.. _development:

Development and Contributing
----------------------------

If you use Selinon and you spot some weird behaviour, feel free to `open an issue <https://github.com/selinon/selinon/issues>`_ .Feel free to dive into source code and submit a pull request to make Selinon a better and more stable project.

Preparing development environment
=================================

To prepare your environment make sure that you have all development requirements installed. Check shipped ``Makefile`` for prepared commands that can be directly issued.

If you would like to create a virtual environment not to install (possibly) unwanted requirements on your system run:

.. code-block:: console

  make venv

To enter the prepared virtual environment run:

.. code-block:: console

  source venv/bin/activate

Now make sure that you have installed all development requirements:

.. code-block:: console

  make devenv

Now you are ready to hack! B-)

Tests
=====

Selinon come with test suites. If you make changes, make sure that the test suite passes:

.. code-block:: console

  make check

The command above will run test suite and report any unexpected behaviour. It will also run linters and some code-quality related tools to ensure your changes look good.

If you make any changes, make sure that the test suite passes before your pull request. If you would like to test changes in some specific situations, `Selinon demo <https:/github.com/selinon/demo>`_ could be a great starter to point to some specific use cases.

And not to forget... If you make any improvements in the source code, feel free to add your name to ``CONTRIBUTORS.txt`` file.


Package documentation
=====================

Automatically generated package documentation is available :mod:`here <selinon>`
