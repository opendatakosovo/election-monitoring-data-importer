class Utils(object):

	@staticmethod
	def to_boolean(arg):
		''' Converting string to boolean
		:param arg: string argument to convert to boolean 
		'''
		if arg == "PO" or arg == "TRUE":
			return True
		elif arg == "JO" or arg == "FALSE":
			return False 
		else:
			return arg		

	@staticmethod
	def to_num(s):
		''' Converting string to integer
		:param arg: string argument to convert to integer
		'''
		try:
			return int(s)
		except ValueError:
			try:
				return float(s)
			except:
				return s
	   
	@staticmethod
	def translate_frequency(term):
		''' Translate frequence term into english. e.g. 'Gjithmone' is 'always'
		'''
		# Use startswith because we don't want to deal with encoding issues (e umlaut).
		# There is probably a more elegant way to deal with this.
		if term.startswith('Gjithmon'):
			return 'always'

		elif term.startswith('Nganj'):

			return 'sometimes'
		elif term.startswith('Rrall'):
			
			return 'rarely'
		elif term.startswith('Aspak'):

			return 'never'
		else:

			return term

	@staticmethod
	def get_csv_filepath(organization, election_year, election_type, election_round):
		''' Generate the CSV filename which contains the data we want to import.
		:param election_year: The observing organization.
		:param election_year: The election year.
		:param election_type: The election type.
		:param elecvtion_round: The election round.
		'''
		return 'data/%s/%s-%s-%s.csv' % (organization, election_year, election_type, election_round)


	@staticmethod
	def get_collection_name(election_year, election_type, election_round):
		''' Generate the name of the collection to import to.
		:param election_year: The election year.
		:param election_type: The election type.
		:param elecvtion_round: The election round.
		'''
		collection_name_with_dashes = '%s%s%s' % (election_type, election_round, election_year)
		collection_name = collection_name_with_dashes.replace('-', '')
		
		return collection_name
		