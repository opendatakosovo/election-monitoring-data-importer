import abc
from dia_importer import DiaImporter

# FIXME: Passing utils as constructor argument because for some reason when we import it from DiaImporter2013 we get this error message:
# 	AttributeError: 'module' object has no attribute 'to_boolean'
#	WHY?!?!
#from emdi import utils

''' UNPROCESSED DATA - START '''

# VOTING ENDS - COUNTING STARTS
'''			
- 49. Were the damaged or refused ballots counted and recorded?"
'''	

# DATA ON COUNTING OF BALLOTS 
'''	
- Conditional ballots (valid only for dual polling stations):
- Number of signatures in the conditional voting list.
- Were the opened envelopes counted separately and were they marked as "invalid"

'''
''' UNPROCESSED DATA - END '''
class DiaImporter2014(DiaImporter):

	def __init__(self, csv_filepath, collection_name, utils):
		DiaImporter.__init__(self, csv_filepath, collection_name, utils)


	def build_voting_center_data(self, row):
		data = [
			row[0], # emri_mbiemri: Name and surname of observer
			row[1], # nr_vezhguesi: Number of Voting Centre
			row[4], # nr_qv: Number of Voting Centre
			row[5], # nr_vv: Voting Station Number
			row[2], # komuna: Municipality
			row[3], # emri_qv: Polling Centre Name
		]

		return data


	def build_on_arrival_data(self, row):
		data = [
			row[6], # pyetja_1: "Is all the propaganda electoral material removed within 100m from the polling center?"
			row[10] # pyetja_2: "Do the disabled people have physical access in the voting center?"
		]

		return data


	def build_preparation_data(self, row):
		data = [
			row[9], # pyetja_3: "Time of your arrival (use the 24-hour system)"
			# "Are the following materials inside and outside the polling station"
			row[7], # pyetja_4: "Information on how to vote?" #FIXME: Need two 4s. Where are they?
			row[7], # yetja_4: "List of candidates from each political entity?" #FIXME: Need two 4s. Where are they?
			row[8], # pyetja_5: "When did preparations in polling station begin? (Members should arrive at 6:00)"
			row[11], # pyetja_6: "Number of PSC members: Total present"
			row[12], # pyetja_femra: "Out of which women"
			row[13], # pyetja_8: "Number of ballots received"
			row[14], # pyetja_9: "Number of voters in voters' list at the polling station"
			row[15], # pyetja_10: "Number of voting booths"
			row[19], # pyetja_14: "Are the booths placed in such a way to ensure vote secrecy?"
			row[16], # pyetja_11: "Was the empty ballot box shown as empty before voting starts?"
			row[17], # pyetja_12: "Were the ballot boxes sealed with security seals?"
			row[18]  # pyetja_13: "Were the serial numbers of seals recorded in the appropriate book?"
		]

		return data


	def build_missing_materials_data(self, row):
		#FIXME: In form but not stored.
		# "If any necessary material was missing in the polling station, write X in the box if it is missing:"
		data = [
			# Ultraviolet lamp
			# Invisible ink
			# Voter's list
			# Ballots
			# Stamp
			# Ballot box
			# Poll Book
			# Voting Booth
			# Envelopes for conditional vote.
		]

		return data


	def build_voting_process_data(self, row):
		voting_process = [
			row[20], # pyetja_15: "When did the polling station open? (Should open at 07:00)"
			row[21], # pyetja_17: "At the entrance, voters are checked with UV-lamp?"
			row[22], # pyetja_18: "Voters are identified with IDs in voters list?"
			row[23], # pyetja_19: "Voter's fingers is sprayed"
			row[24], # pyetja_20: "The ballot of voters is stamped"
			row[25], # pyetja_10_21: "How many voters voted by 10:00"
			row[26], # pyetja_12_21: "How many voters voted by 13:00"
			row[27], # pyetja_16_21: "How many voters voted by 16:00"
			row[28], # pyetja_19_21: "How many voters voted by 19:00"
			row[29], # pyetja_22: "How many voters did NOT find their name in the Voter List?"
			row[30], # pyetja_23: "What is the number of conditional voters (only if you are in a polling station where this type of voting is provided)?"
			row[31], # pyetja_24: "What is the number of voters who voted with assistance?"
			row[32], # pyetja_25: "Were at least three PSC members present at all times:"
			'' # Comments. In Form but not stored.
		]

		return voting_process

	def build_refused_ballots_data(self, row):
		
		data = [
			row[57], # pyetja_49: "Were the damaged or refused ballots counted and recorded"
			'', # total count not in form.
		]

		return data


	def build_voting_observers_data(self, row):
		#FIXME: In form but not stored.
		data = []
		return data


	def build_irregularities_data(self, row):
		data = [
			# FIXME: Value can be 0, 1, 2. Figure out which ones are No, Yes/No, and Yes/Yes
			self.utils.to_boolean(row[33]), # pyetja_26: "Did anyone try to vote more than once?"
			self.utils.to_boolean_second(row[33]), # pyetja_26: "If yes, was he/she allowed to vote?"
			# FIXME: Value can be 0, 1, 2. What does this mean?			
			row[34], # pyetja_27: "Did anyone try to photograph the ballot?"
			row[35], # pyetja_28: "Did any voter insert more than one ballot in the box?"
		 	row[36], # pyetja_29: "Unauthorized persons (without badge) stayed in polling station?"
			row[37], # pyetja_30: "There was violence or threats in polling station?"
			row[38], # pyetja_31: "There was political propaganda inside the polling station?"
			row[39], # pyetja_32: "More than 1 person behind the booth (family voting)?"
			row[40], # pyetja_33: "Was the polling station closed at any one point during the day?"
			row[41], # pyetja_34: "Was there voting outside the booths?"
			row[45]  # pyetja_37: "Were there any incidents during the election process?"
		]

		return data


	def build_complaints_data(self, row):
		data = [
			row[42], # pyetja_35: "How many voters complained during the voting process"
			row[43], # pyetja_36: "How many voters completed the complaint form?"
			row[44]  # In form but not stored. pyetja_36a: "If there was a complaint form, were PSC impartial in responding to complaints?"
		]

		return data


	def build_voting_end_data(self, row):
		data = [
			row[46], # pyetja_38: "When did voting process end?"
			row[47], # pyetja_39: "Were there any people waiting in queue when voting center/station closed?"
			row[48], # pyetja_40: "Were they allowed to vote?"
			self.utils.to_counting_begin_time_range(row[49]), # pyetja_41: "When did the counting begin?"
			row[50], # pyetja_42: "Were there any unauthorized persons present during counting?"
			'', # pyetja_??: "If yes, who" #FIXME
			row[51], # pyetja_43: "Could the observer have a clear view of the procedures?"
			row[52], # pyetja_44: "Were the security seals checked before opening the ballot box?"
			row[53], # pyetja_45: "Were the security seals intact?"
			row[54], # pyetja_46: "Were the signatures in the voters list counted and recorded?"
			row[55], # pyetja_47: "Number of signature in the voters' list (how many persons voted)?"
			row[56], # pyetja_48: "Were the unused ballots counted and registered?"
			row[58], # pyetja_50: "Were the security seals verified and registered?"
			row[59]  # pyetja_51: "Were the voting materials moved aside (unused/damaged/refused ballots, stamps, voters' list, markers, pencils)?"
		]

		return data



	def build_counting_ballots_data(self, row):
		data = [
			row[60], # pyetja_52: "Total number of ballots in the ballot box"
			row[61], # pyetja_53: "How many invalid ballots were in the ballot box?"
			row[62], # pyetja_54: "Were the invalid ballots moved aside?"
			row[63], # pyetja_55: "After counting the ballots, were they placed in a transparent plastic bag?"
		]	
		return data


	def build_counting_summary_data(self, row):
		data = [
			row[64], # pyetja_56: "Was the decision on dubious ballots just?"
			row[65], # pyetja_57: "In case of disagreement, were they written in the book?"
			self.utils.to_counting_finish_time_range(row[66]), # pyetja_58: "When did the counting finish?" #FIXME: 5 options instead of just 4: 0,1,2,3,4
			row[67], # pyetja_59: "Did any representative or observer oppose the results for this polling station?"
			'', # "If yes, who" # FIXME: Three columns for this: pytja_kusht1, pytja_kusht2, and pytja_kusht3. 
			#FIXME: The data for comments is either blank, 0, or 1.
			row[71]  # pyetja_60: "Other comments: (If there is a discrepancy between the number of signatures and ballots in the box, explain why)?" 
		]

		return data

	def build_results_object(self, row):
		return {}

