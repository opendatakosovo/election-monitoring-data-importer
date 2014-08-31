class Utils(object):

	@staticmethod
	def to_boolean(arg):
		''' Converting string to boolean
		:param arg: string argument to convert to boolean 
		'''
		if arg == True:
			return True	
		elif arg == False:
			return False

		elif arg == "PO" or arg == "TRUE":
			return True
		elif arg == "JO" or arg == "FALSE":
			return False 

		elif arg == "0":
			return False
		elif arg == "1" or arg == "2":
			return True

		else:
			return ''

	@staticmethod
	def to_boolean_second(arg):
		''' Converting second val to boolean.
			0 -> False/False
			2 -> True/False
			3 -> True/True
		:param arg: string argument to convert to boolean 
		'''
		if arg == True:
			return True	
		elif arg == False:
			return False

		elif arg == "1":
			return False
		elif arg == "2":
			return True

		else:
			return ''

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
				return ''

		return ''
	   
	@staticmethod
	def translate_frequency(term):
		''' Translate frequence term into english. e.g. 'Gjithmone' is 'always'
		'''
		# Use startswith because we don't want to deal with encoding issues (e umlaut).
		# There is probably a more elegant way to deal with this.
		if term.startswith('Gjithmon') or term == '1':
			return 'always'

		elif term.startswith('Nganj') or term == '2':
			return 'sometimes'

		elif term.startswith('Rrall') or term == '3':
			return 'rarely'

		elif term.startswith('Aspak') or term == '4':
			return 'never'

		else:
			return ''


	@staticmethod
	def to_counting_begin_time_range(arg):
		if arg == '0':
			return "Before 19:00"

		elif arg == '1':
			return "19:00 - 20:00"

		elif arg == '2':
			return "20:00 - 21:00"

		elif arg == '3':
			return "21:00 - 22:00"

		else:
			return ''


	@staticmethod
	def to_counting_finish_time_range(arg):
		if arg == '0':
			return "19:00 - 20:00"

		elif arg == '1':
			return "20:00 - 21:00"

		elif arg == '2':
			return "21:00 - 23:00"

		elif arg == '3' or arg == '4':
			return "After 23:00"

		else:
			return ''


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
		
