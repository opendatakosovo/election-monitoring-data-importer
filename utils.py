class Utils(object):

	def __init__(self):
    		pass

	def to_boolean(self, arg):
		''' Converting string to boolean
		:param arg: string argument to convert to boolean 
		'''
		if arg == "PO" or arg == "TRUE":
			return True
		elif arg == "JO" or arg == "FALSE":
			return False 
		else:
			return arg		

	def to_num(self, s):
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
	   
	def translate_frequency(self, term):
		''' Translate frequence term into english. e.g. 'Gjithmone' is 'always'
		'''
		# Use startswith because we don't want to deal with encoding issues (e umlaut).
		# There is probably a more elegant way to deal with this.
		if term.startswith('Gjithmon'):
			return 'always'
		else:
			return term
		#TODO: Cover the other terms
