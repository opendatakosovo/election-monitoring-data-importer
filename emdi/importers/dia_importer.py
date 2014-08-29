from pymongo import MongoClient

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
