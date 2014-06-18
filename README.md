kdi-elections-monitoring-data-importer
======================================

KDI Election Monitoring data importer. Imports collected data to MongoDB.

Set Up Environment
==================
1. Install cURL. Required to download Python and/or virtualenv (e.g. in Ubuntu: sudo apt-get install curl).
2. Install python-dev. Required to compile 3rd party python libraries. In this case PyMongo. If not installed will result in [this error](http://www.cyberciti.biz/faq/debian-ubuntu-linux-python-h-file-not-found-error-solution/) (e.g. in Ubuntu: sudo apt-get install python-dev).
3. TODO: These one-time installations should be included in the install.sh script (like for Python and virtualenv).

Install Application
===================
1. Install the app: sudo bash install.sh

Run
===
1. Start mongo server.
2. Run the app: bash run.sh

