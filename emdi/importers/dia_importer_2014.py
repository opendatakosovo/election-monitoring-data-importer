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

				# Build JSON objects.
				# The methods that build the data object are implemted in this class
				# The methods that build the JSON object are implmenet in the super-class

				polling_station_data = self.build_polling_station_data(row) # Implementation in this class
				polling_station = self.build_polling_station_object(polling_station_data) # Implementation in the super-class

				preparation_data = self.build_preparation_data(row) # Implementation in this class
				missing_materials_data = self.build_missing_materials_data(row) # Implementation in this class
				preparation = self.build_preparation_object(preparation_data, missing_materials_data) # Implementation in the super-class

				# Different implementation required to build the data object for each sub-class.
				# So in this case, we don't create a generic data object creation method in the super-class
				voting_process = self.build_voting_process_object(row) # Implementation in this class

				irregularities_data = self.build_irregularities_data(row) # Implementation in this class
				irregularities = self.build_irregularities_object(irregularities_data) # Implementation in the super-class

				complaints_data = self.build_complaints_data(row) # Implementation in this class
				complaints = self.build_complaints_object(complaints_data) # Implementation in the super-class

				# Build observation documents
				observation = {
					'_id': str(ObjectId()),
					'pollingStation': polling_station,
					'preparation': preparation,
					'votingProcess': voting_process,
					'irregularities': irregularities,				
					'complaints': complaints,
				}

				# Insert document
				self.mongo.kdi[self.collection_name].insert(observation)
				num_of_created_docs = num_of_created_docs + 1
			# End for

		return num_of_created_docs


	def build_polling_station_data(self, row):
		data = [
			row[0], # column name: emri_mbiemri
			row[1], # colun name: nr_vezhguesi
			row[4], # Column name: nr_qv
			row[5], # Column name: nr_vv
			row[2], # Column name: komuna
			row[3], # Column name: emri_qv
		]

		return data


	def build_preparation_data(self, row):
		data = [
			row[9], # column name: pyetja_3 
			row[7], # column name: pyetja_4 #FIXME: Need two 4s. Where are they?
			row[7], # column name: pyetja_4 #FIXME: Need two 4s. Where are they?
			row[8], # column name: pyetja_5
			row[11],# column name: pyetja_6
			row[12] # column name: pyetja_femra
		]

		return data



	def build_missing_materials_data(self, row):
		data = [
			row[15],
			row[16],
			row[17],
			row[18],
			row[19],
			row[20],
			row[21],
			row[22],
			row[23]
		]

		return data


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
				#'refusedBallot' : {
				#	'refused': self.utils.to_boolean(row[56]), #column name
				#	'count': self.utils.to_num(row[57]) #column name 
				#}
			},
			'atLeastThreePscMembersPresentInPollingStation' : self.utils.to_boolean(row[33]), #column name
			# IN FORM BUT NOT STORED
			#'comments' : row[58]  # column name
		}

		return voting_process


	def build_irregularities_data(self, row):
		data = [
			# TODO: Value can be 0, 1, 2. Figure out which ones are No, Yes/No, and Yes/Yes
			self.utils.to_boolean(row[33]),
			self.utils.to_boolean_second(row[33]),
			row[34],
			row[35],
		 	row[36],
			row[37],
			row[38],
			row[39],
			row[40],
			row[41],
			row[45]
		]

		return data


	def build_complaints_data(self, row):
		data = [
			row[42], # Column name: PA10VAV
			row[43], # Column name: PA11VMF
			row[44]  #column name PA12PAA
		]

		return data
