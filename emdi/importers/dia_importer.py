from pymongo import MongoClient
from slugify import slugify

class DiaImporter(object):

	def __init__(self, csv_filepath, collection_name, utils):
		self.collection_name = collection_name
		self.csv_filepath = csv_filepath

		self.mongo = MongoClient()

		# Clear collection prior to import.
		self.mongo.kdi[collection_name].remove({})

		# FIXME: Passing utils as constructor argument because for some reason when we import it from DiaImporter2013 we get this error message:
		# 	AttributeError: 'module' object has no attribute 'to_boolean'
		#	WHY?!?!
		self.utils = utils

	def build_polling_station_object(self, data):
		polling_station = {
			'name': data[5].strip(),
			'slug': slugify(data[5].strip()),
			'number': data[2].upper(),
			'room': data[3].upper(),
			'observer':{
				'name' : data[0].strip(),
				'number' : data[1].strip(),
				'slug' : slugify(data[0].strip()),
			},
			'commune':{
				'name': data[4].strip(),
				'slug': slugify(data[4].strip())
			}
		}

		return polling_station


	def build_preparation_object(self, prep_data, missing_material_data):
		preparation = {
			'observerArrivalTime':prep_data[0],
			'materialsInsideAndOutsiteVotingStation':{
				'howToVoteInfo': self.utils.to_boolean(prep_data[1]),
				'listOfCandidates': self.utils.to_boolean(prep_data[2])
			},
			'preparationStartTime': prep_data[3],
			'pscMembers':{
				'total': self.utils.to_num(prep_data[4]),
				'female': self.utils.to_num(prep_data[5])
			},
			'missingMaterials':{
				'uvLamp': self.utils.to_boolean(missing_material_data[0]),
				'invisibleInk':self.utils.to_boolean(missing_material_data[1]),
				'votersList': self.utils.to_boolean(missing_material_data[2]),
				'ballots': self.utils.to_boolean(missing_material_data[3]),
				'stamp': self.utils.to_boolean(missing_material_data[4]),
				'ballotBox':self.utils.to_boolean(missing_material_data[5]),
				'pollBook': self.utils.to_boolean(missing_material_data[6]),
				'votingBooth': self.utils.to_boolean(missing_material_data[7]),
				'envelopesForConditionalVote': self.utils.to_boolean(missing_material_data[8])
			}
		}

		return preparation




	def build_irregularities_object(self, data):
		irregularities = {
			'moreThanOneVote':{
				'attempted': data[0],
				'allowedToVote': data[1]
			},
			'photographedBallot': self.utils.to_boolean(data[2]),
			'personInsertedMoreThanOneBallot': self.utils.to_boolean(data[3]),
		 	'unauthorizedPersonsStayedInPollingStation': self.utils.to_boolean(data[4]),
			'violenceOrThreats': self.utils.to_boolean(data[5]),
			'politicalPropagandaInsideThePollingStation': self.utils.to_boolean(data[6]),
			'moreThanOnePersonBehindTheBooth': self.utils.to_boolean(data[7]),
			'pollingStationClosedAtAnyPointDuringDay': self.utils.to_boolean(data[8]),
			'caseVotingOutsideTheCabin': self.utils.to_boolean(data[9]),
			'accidents': self.utils.to_boolean(data[10])
		}

		return irregularities


	def build_complaints_object(self, data):

		complaints = {
			'total': self.utils.to_num(data[0]),
			'filled': self.utils.to_num(data[1]),
			'pscImpartiality' : self.utils.translate_frequency(data[2]),
		}

		return complaints
