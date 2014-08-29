Elections Monitoring Data Importer
==================================

KDI Election Monitoring data importer. Imports collected data to MongoDB.

Set Up Environment
==================
1. Install cURL. Required to download Python and/or virtualenv (e.g. in Ubuntu: sudo apt-get install curl).
2. Install python-dev. Required to compile 3rd party python libraries. In this case PyMongo. If not installed will result in [this error](http://www.cyberciti.biz/faq/debian-ubuntu-linux-python-h-file-not-found-error-solution/) (e.g. in Ubuntu: sudo apt-get install python-dev).


Install Application
===================
1. Install the app: sudo bash install.sh

Run
===
1. Start mongo server.
2. Run the importer:

Import Democracy in Action's 2013 local elections monitoring data:
	bash run.sh --organization=dia --electionYear=2013 --electionType=local-elections --electionRound=first-round

Import Democracy in Action's 2013 local elections monitoring data:
	bash run.sh --organization=dia --electionYear=2013 --electionType=local-elections --electionRound=second-round

Import Democracy in Action's 2014 general elections monitoring data:
	bash run.sh --organization=dia --electionYear=2014 --electionType=general-elections --electionRound=first-round

