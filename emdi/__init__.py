import os
import argparse
from utils.utils import Utils
from termcolor import colored

from importers.dia_importer_2013 import DiaImporter2013
from importers.dia_importer_2014 import DiaImporter2014

parser = argparse.ArgumentParser()
parser.add_argument('--organization', type=str, help='The election monitoring organization.')
parser.add_argument('--electionYear', type=str, help='The year of the election.')
parser.add_argument('--electionType', type=str, help='The type of the election (local-elections or general-elections).')
parser.add_argument('--electionRound', type=str, help='The round of the election (firt-round or second-round).')

# Parse arguemnts and run the app.	
args = parser.parse_args()

organization = args.organization
election_year = args.electionYear
election_type = args.electionType
election_round = args.electionRound

# Create utils instance.
utils = Utils()

def import_data():
	''' Imports observation data from a CSV file into a MongoDB collection.
	'''
	csv_filepath = utils.get_csv_filepath(organization, election_year, election_type, election_round)
	collection_name = utils.get_collection_name(election_year, election_type, election_round)

	if os.path.isfile(csv_filepath):
		print 'Importing from %s' % csv_filepath,

		try:
			importer = None

			# FIXME: Implement AbstractFactory pattern to instanciate appropriate importer object
			if organization == 'dia':
				if election_year == '2013':
					# FIXME: Passing utils as constructor argument because for some reason when
					# we import it from DiaImporter2013 we get this error message:
					# 		AttributeError: 'module' object has no attribute 'to_boolean'
					#
					# WHY?!?!
					importer = DiaImporter2013(csv_filepath, collection_name, utils)

				elif election_year == '2014':
					importer = DiaImporter2014(csv_filepath, collection_name, utils)

			# Run the importer
			num_of_docs_created = importer.execute()

			print colored('		[OK]', 'green', attrs=['bold'])

		except:
			print colored('		[FAIL]', 'red', attrs=['bold'])
			raise
	else:
		print colored('INPUT ERROR: No CSV data file exists for those given parameters.', 'red')
	
