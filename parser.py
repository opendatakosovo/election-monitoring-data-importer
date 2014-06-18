import csv

from pymongo import MongoClient

csv_filename = 'kdi-local-elections-observations-first-round-2013.csv'

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.localelections2013
collection = db.kdifirstroundobservations

# Clear data
db.kdifirstroundobservations.remove({})

def parse_csv():
	'''
	Reads the KDI local election monitoring CSV file.
	Creates Mongo document for each observation entry.
	Stores generated JSON documents.
	'''
	with open(csv_filename, 'rb') as csvfile:
		reader = csv.reader(csvfile)
		
		# Skip the header
		next(reader, None)
		
		# Iterate through the rows, retrieve desired values.
		for row in reader:
			polling_station_number = row[3] # Column name: nrQV
			room_number = row[4] # Column name: NRVV
			
			commune = row[5]
			polling_station_name = row[6]
			
			ultra_violet_control = row[45] # Column name: PV03UVL
			identified_with_document = row[46] # Column name: PV04IDK
			finger_sprayed = row[47] # Column name: PV05GSH
			sealed_ballot = row[48] # Column name: PV06VUL
			
			how_many_voted_by_ten_AM = row[49] # Column name: PV07-10
			how_many_voted_by_one_PM = row[50] # Column name: PV07-13
			how_many_voted_by_four_PM = row[51] # Column name: PV07-16
			how_many_voted_by_seven_PM = row[52] # Column name: PV07-19
			
			observation = {
				'pollingStation':{
					'number': polling_station_number,
					'roomNumber': room_number,
					'name': polling_station_name,
					'commune': commune
				},
				'votingProcess':{
					'voters':{
						'ultraVioletControl': ultra_violet_control,
						'identifiedWithDocument': identified_with_document,
						'fingerSprayed': finger_sprayed,
						'sealedBallot': sealed_ballot, #FIXME: Store as int instead of string.
						'howManyVotedBy':{
							'tenAM': how_many_voted_by_ten_AM,
							'onePM': how_many_voted_by_one_PM,
							'fourPM': how_many_voted_by_four_PM,
							'sevenPM': how_many_voted_by_seven_PM
						}
					}
				}
			}
			
			collection.insert(observation)

parse_csv()	
