import csv
from bson import ObjectId
from slugify import slugify

from dia_importer import DiaImporter

class DiaImporter2014(DiaImporter):

	def __init__(self, csv_filepath, collection_name, utils):
		DiaImporter.__init__(self, csv_filepath, collection_name, utils)

	def run(self):
		'''
		Reads the KDI local election monitoring CSV file.
		Creates Mongo document for each observation entry.
		Stores generated JSON documents.
		'''
		
		num_of_created_docs = 0

		with open(self.csv_filepath, 'rb') as csvfile:
			reader = csv.reader(csvfile)
			# Skip the header
			next(reader, None)

			# Iterate through the rows, retrieve desired values.
			for row in reader:

				polling_station = self.build_polling_station_object(row)
				missing_materials = self.build_missing_materials_object(row)
				voting_process = self.build_voting_process_object(row)
				irregularities = self.build_irregularities_object(row)
				complaints = self.build_complaints_object(row)

				observation = {
					'_id': str(ObjectId()),
					'pollingStation': polling_station,
					'missingMaterial': missing_materials,
					'votingProcess': voting_process,
					'irregularities': irregularities,				
					'complaints': complaints,
				}

				# Insert document
				self.mongo.kdi[self.collection_name].insert(observation)
				num_of_created_docs = num_of_created_docs + 1
			# End for

		return num_of_created_docs


	def build_polling_station_object(self, row):
		polling_station = {
			'observerName' : row[0], #column name emri_mbiemri
			'observerNumber' : row[1], #colun name nr_vezhguesi
			'number': row[4].lower(), # Column name nr_qv
			'roomNumber': row[5].lower(), # Column name nr_vv
			'commune': row[2].strip(), # Column name komuna
			'communeSlug': slugify(row[2].strip()),
			'name': row[3].strip(), # Column name emri_qv
			'nameSlug': slugify(row[3].strip()) 
		}

		return polling_station


	def build_missing_materials_object(self, row):
		missing_materials = {
			'uvLamp': self.utils.to_boolean(row[15]), # Column name 
			'spray':self.utils.to_boolean(row[16]), # Column name 
			'votersList': self.utils.to_boolean(row[17]), # Column name 
			'ballots': self.utils.to_boolean(row[18]), # Column name 
			'stamp': self.utils.to_boolean(row[19]), # Column name 
			'ballotBox':self.utils.to_boolean(row[20]), # Column name 
			'votersBook': self.utils.to_boolean(row[21]), # Column name 
			'votingCabin': self.utils.to_boolean(row[22]), # Column name 
			'envelopsForConditionVoters': self.utils.to_boolean(row[23]) # Column name 
		}

		return missing_materials


	def build_voting_process_object(self, row):
		voting_process = {
			'whenVotingProcessStarted' : row[31], # Column name
			# IN FORM BUT NOT STORED
			#'observersPresent':{
			#		'pdk': self.utils.to_boolean(row[32]), # Column name
			#		'ldk': self.utils.to_boolean(row[33]), # Column name
			#		'lvv': self.utils.to_boolean(row[34]), # Column name
			#		'aak': self.utils.to_boolean(row[35]), # Column name
			#		'akr': self.utils.to_boolean(row[36]), # Column name
			#		'otherParties' : filter(None, row[37:40]), #
			#		'ngo': self.utils.to_boolean(row[40]), # 
			#		'media': self.utils.to_boolean(row[41]), #
			#		'international': self.utils.to_boolean(row[42]), #
			#		'other': row[43] # 
			#},
			'voters':{
				'ultraVioletControl': self.utils.translate_frequency(row[22]), # Column name
				'identifiedWithDocument': self.utils.translate_frequency(row[23]), # Column name
				'fingerSprayed': self.utils.translate_frequency(row[24]), # Column name
				'sealedBallot': self.utils.translate_frequency(row[25]), # Column name
				'howManyVotedBy':{
					'tenAM': self.utils.to_num(row[26]), # Column name
					'onePM': self.utils.to_num(row[27]), # Column name
					'fourPM': self.utils.to_num(row[28]), # Column name
					'sevenPM': self.utils.to_num(row[29]) # Column name
				},
				'notInVotersList' : self.utils.to_num(row[30]),  #column name
				'conditional' : self.utils.to_num(row[31]), #column name
				'assisted' : self.utils.to_num(row[32]), #column name
				# NOT IN FORM
				#'ballot' : {
				#	'refused': self.utils.to_boolean(row[56]), #column name
				#	'count': self.utils.to_num(row[57]) #column name 
				#}
			},
			'atLeastThreeKvvMembersPresentInPollingStation' : self.utils.to_boolean(row[33]), #column name
			# IN FORM BUT NOT STORED
			#'comments' : row[58]  # column name
		}

		return voting_process


	def build_irregularities_object(self, row):
		irregularities = {
			# Value can be 0, 1, 2. Figure out which ones are No, Yes/No, and Yes/Yes
			'attemptToVoteMoreThanOnce':self.utils.to_boolean(row[33]), #Column name: PA01x1
			'allowedToVote':self.utils.to_boolean_second(row[33]), #Column name: PAifPO

			'photographedBallot':self.utils.to_boolean(row[34]), #Column name: PA02Fot
			'insertedMoreThanOneBallot':self.utils.to_boolean(row[35]), #Column name: PA03M1F
		 	'unauthorizedPersonsStayedAtTheVotingStation': self.utils.to_boolean(row[36]), #Column name: PA04PPD
			'violenceInTheVotingStation': self.utils.to_boolean(row[37]), #Column name: PA05DHU
			'politicalPropagandaInsideTheVotingStation': self.utils.to_boolean(row[38]), #Column name: PA06PRP
			'moreThanOnePersonBehindTheCabin': self.utils.to_boolean(row[39]),  #Column name: PA07M1P
			'hasTheVotingStationBeenClosedInAnyCase': self.utils.to_boolean(row[40]), #Column name: PA08MBV
			'caseVotingOutsideTheCabin': self.utils.to_boolean(row[41]), #Column name: PA09VJK
			'areTheKvvMembersImpartialWhenTheyReactToComplaints' : self.utils.translate_frequency(row[44]),  #column name PA12PAA
			'anyAccidentHappenedDuringTheProcess': self.utils.to_boolean(row[45]) #Column name: PA13INC
		}

		return irregularities


	def build_complaints_object(self, row):

		complaints = {
			'total': self.utils.to_num(row[42]), # Column name: PA10VAV
			'filled': self.utils.to_num(row[43]) # Column name: PA11VMF
		}

		return complaints
