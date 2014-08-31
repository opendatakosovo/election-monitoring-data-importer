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

				voting_process_data = self.build_voting_process_data(row) # Implementation in this class
				observers_data = self.build_observers_data(row) # Implementation in this class
				refused_ballots_data = self.build_refused_ballots_data(row) # Implementation in this class
				voting_process = self.build_voting_process_object(voting_process_data, observers_data, refused_ballots_data) # Implementation in the super-class

				irregularities_data = self.build_irregularities_data(row) # Implementation in this class
				irregularities = self.build_irregularities_object(irregularities_data) # Implementation in the super-class

				complaints_data = self.build_complaints_data(row) # Implementation in this class
				complaints = self.build_complaints_object(complaints_data) # Implementation in the super-class

				# Build observation documents
				observation = {
					'_id': str(ObjectId()),
					'pollingStation': polling_station,
					'preparation': preparation,
					'process': {
						'voting': voting_process,
						'counting': {}
					},
					'irregularities': irregularities,				
					'complaints': complaints
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
		data = []

		return data


	def build_voting_process_data(self, row):
		voting_process = [
			row[20],
			row[21],
			row[22],
			row[23],
			row[24],
			row[25],
			row[26],
			row[27],
			row[28],
			row[29],
			row[30],
			row[31],
			row[32],
			'' # Comments. In Form but not stored.
		]

		return voting_process

	def build_refused_ballots_data(self, row):
		data = []

		return data


	def build_observers_data(self, row):
		data = []

		return data


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
			row[42],
			row[43],
			row[44] 
		]

		return data
