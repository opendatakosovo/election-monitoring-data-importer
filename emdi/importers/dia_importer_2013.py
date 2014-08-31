import csv
from bson import ObjectId
from slugify import slugify

from dia_importer import DiaImporter

# FIXME: Passing utils as constructor argument because for some reason when we import it from DiaImporter2013 we get this error message:
# 	AttributeError: 'module' object has no attribute 'to_boolean'
#	WHY?!?!
#from emdi import utils

class DiaImporter2013(DiaImporter):

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
				
				''' UNPROCESSED DATA - START '''

				'''
				additional_comments = row[134]	#KomShtese
				'''

				# DURING THE ARRIVAL OF THE VOTING MATERIAL
				'''
				number_of_accepted_ballots = row[24] # column name:P06NFP
				number_of_voters_in_voting_station_list = row[25] # column name: P07VNL
				number_of_voting_cabins=row[26] # column name:P08NKV
				votingbox_shown_empty=row[27] # column name:P09TKZ
				closed_with_safetystrip = row[28] # column name:P10SHS
				did_they_register_serial_number_of_strips = row[29] # column name:P11NRS
				cabins_provided_voters_safety_and_privancy = row[30] # column name:P12KFV
				'''	

				# VOTING PROCESS END - COUNTING STARTS
				'''
				row[95], # PM11NVL: WHAT'S THIS? whats_number_of_voters_in_that_polling_station
				row[98] # PM14PSH: #FIXME: did_they_count_and_register_used_ballots??"
				'''

				'''
				pdk_observers = row[77] #column name PM05-PDK	
				ldk_observers = row[78] #column name PM05-LDK
				lvv_observers = row[79] #column name PM05-LVV	
				aak_observers = row[80] #column name PM05-AAK
				akr_observers = row[81] #column name PM05-AKR

				other_parties_observers_1 = row[82] #column name PM05-TJ1
				other_parties_observers_2 = row[83] #column name PM05-TJ2
				other_parties_observers_3 = row[84] #column name PM05-TJ3

				other_parties = [other_parties_observers_1, other_parties_observers_2, other_parties_observers_3]
				other_parties = filter(None, other_parties)

				ngo_observers = row[85] #column name PM05-OJQ
				media_observers = row[86] #column name PM05-MED
				international_observers = row[87] #column name PM05-VZH
				other_observers = row[88] #column name PM05-TJE
				'''

				''' UNPROCESSED DATA - END '''


				# Build JSON objects.
				# The methods that build the data object are implemted in this class
				# The methods that build the JSON object are implmenet in the super-class

				# Build 'votingCenter' object.
				voting_center_data = self.build_voting_center_data(row) # Implementation in this class
				voting_center = self.build_voting_center_object(voting_center_data ) # Implementation in the super-class

				# Build 'onArrival' object.
				on_arrival_data = self.build_on_arrival_data(row) # Implementation in this class
				on_arrival = self.build_on_arrival_object(on_arrival_data) # Implementation in the super-class

				# Build 'preparation' object.
				preparation_data = self.build_preparation_data(row) # Implementation in this class
				missing_materials_data = self.build_missing_materials_data(row) # Implementation in this class
				preparation = self.build_preparation_object(preparation_data, missing_materials_data) # Implementation in the super-class

				# Build 'voting.process' object.
				voting_process_data = self.build_voting_process_data(row) # Implementation in this class
				observers_data = self.build_voting_observers_data(row) # Implementation in this class
				refused_ballots_data = self.build_refused_ballots_data(row) # Implementation in this class
				voting_process = self.build_voting_process_object(
					voting_process_data,
					observers_data,
					refused_ballots_data) # Implementation in the super-class

				# Build 'voting.irregularities' object.
				irregularities_data = self.build_irregularities_data(row) # Implementation in this class
				irregularities = self.build_irregularities_object(irregularities_data) # Implementation in the super-class

				# Build 'voting.complaints' object.
				complaints_data = self.build_complaints_data(row) # Implementation in this class
				complaints = self.build_complaints_object(complaints_data) # Implementation in the super-class

				# Build 'voting.end' object.
				voting_end_data = self.build_voting_end_data(row)  # Implementation in this class
				voting_end = self.build_voting_end_object(voting_end_data)  # Implementation in the super-class

				# Build 'counting.ballots' object.
				counting_ballots_data = self.build_counting_ballots_data(row)  # Implementation in this class
				counting_ballots = self.build_counting_ballots_object(counting_ballots_data) # Implementation in the super-class

				# Build 'counting.summary' object.
				counting_summary_data = self.build_counting_summary_data(row)  # Implementation in this class
				counting_summary = self.build_counting_summary_object(counting_summary_data) # Implementation in the super-class

				results = self.build_results_object(row) # Implementation in this class

				observation = {
					'_id': str(ObjectId()),
					'votingCenter': voting_center,
					'onArrival': on_arrival,
					'preparation': preparation,
					'voting': {
						'process': voting_process,
						'irregularities': irregularities,				
						'complaints': complaints,
						'concludes': voting_end
					},
					'counting':{
						'ballots': counting_ballots,
						'summary': counting_summary
					},
					'results': results
				} 
			
				# Insert document
				self.mongo.kdi[self.collection_name].insert(observation)
				num_of_created_docs = num_of_created_docs + 1

		return num_of_created_docs


	def build_voting_center_data(self, row):
		data = [
			row[1], # EmriV: Name and surname of observer
			row[2], # NrV: Number of Voting Centre
			row[3], # nrQV: Number of Voting Centre
			row[4], # NRVV: Voting Station Number
			row[5], # Komuna: Municipality
			row[6], # EQV: Polling Centre Name
		]

		return data


	def build_on_arrival_data(self, row):
		data = [
			row[7], # 01gja: "Is all the propaganda electoral material removed within 100m from the polling center?"
			row[8] # 02gja: "Do the disabled people have physical access in the voting center?"
		]

		return data


	def build_preparation_data(self, row):
		data = [
			row[9],  # P01KA: "Time of your arrival (use the 24-hour system)"
			row[10], # P02A: "Information on how to vote?"
			row[11], # P02B: "List of candidates from each political entity?"
			row[12], # P03Perg: "When did preparations in polling station begin? (Members should arrive at 6:00)"
			row[13], # P04KVV: "Number of PSC members: Total present"
			row[14], # P04Fem: "Out of which women"
			row[24], # P06NFP: "Number of ballots received"
			row[25], # P07VNL: "Number of voters in voters' list at the polling station"
			row[26], # P08NKV: "Number of voting booths"
			row[30], # P12KFV: "Are the booths placed in such a way to ensure vote secrecy?"
			row[27], # P09TKZ: "Was the empty ballot box shown as empty before voting starts?"
			row[28], # P10SHS: "Were the ballot boxes sealed with security seals?"
			row[29]  # P11NRS: "Were the serial numbers of seals recorded in the appropriate book?"
		]

		return data;


	def build_missing_materials_data(self, row):
		data = [
			row[15], # P05Lla: Ultraviolet lamp
			row[16], # P05Ngj: Invisible ink
			row[17], # P05Lis: Voter's list
			row[18], # P05Flv: Ballots
			row[19], # P05Vul: Stamp
			row[20], # P05Kut: Ballot box
			row[21], # P05Lib: Poll Book
			row[22], # P05Kab: Voting Booth
			row[23]  # P05ZFK: Envelopes for conditional vote.
		]

		return data;


	def build_voting_process_data(self, row):
		voting_process = [
			row[31], # PV1KHV: "When did the polling station open? (Should open at 07:00)"
			row[44], # PV03UVL: "At the entrance, voters are checked with UV-lamp?"
			row[45], # PV04IDK: "Voters are identified with IDs in voters list?"
			row[46], # PV05GSH: "Voter's fingers is sprayed"
			row[47], # PV06VUL: "The ballot of voters is stamped"
			row[48], # PV07-10: "How many voters voted by 10:00"
			row[49], # PV07-13: "How many voters voted by 13:00"
			row[50], # PV07-16: "How many voters voted by 16:00"
			row[51], # PV07-19: "How many voters voted by 19:00"
			row[52], # PV08ELV: "How many voters did NOT find their name in the Voter List?"
			row[53], # PV09NVK: "What is the number of conditional voters (only if you are in a polling station where this type of voting is provided)?"
			row[54], # PV10VAS: "What is the number of voters who voted with assistance?"
			row[55], # PV11-3AN: "Were at least three PSC members present at all times:"
			row[58]  # ProcVotKom: "Comments"
		]

		return voting_process


	def build_refused_ballots_data(self, row):
		data = [
			row[56], # PV12_Ref: "Were the damaged or refused ballots counted and recorded"
			row[57]  # PV12IFPo: 
		]

		return data


	def build_voting_observers_data(self, row):
		data = [
			'PDK' if self.utils.to_boolean(row[32]) else None,  # PDK
			'LDK' if self.utils.to_boolean(row[33]) else None, # LDK
			'LVV' if self.utils.to_boolean(row[34]) else None, # LVV
			'AAK' if self.utils.to_boolean(row[35]) else None, # AAK
			'AKR' if self.utils.to_boolean(row[36]) else None,  # AKR
			row[37].upper() if row[37] != None else None, # ParTj01
			row[38].upper() if row[38] != None else None, # ParTj02
			row[39].upper() if row[39] != None else None, # ParTj03
			'NGO' if self.utils.to_boolean(row[40]) else None, # OJQ
			'Media' if self.utils.to_boolean(row[41]) else None, # Media
			'International Observers' if self.utils.to_boolean(row[42]) else None  # VzhND
		]

 		# TODO: How to handle Others?
		# row[43] # VzhTjere

		# e.g. observation with other parties:
		# db.localelectionsfirstround2013.findOne({'pollingStation.number':'1804E'})

		# e.g. others:
		# db.localelectionsfirstround2013.findOne({'pollingStation.number':'1837E'})

		return filter(None, data)


	def build_irregularities_data(self, row):
		data = [
			self.utils.to_boolean(row[59]), # PA01x1: "Did anyone try to vote more than once?"
			self.utils.to_boolean(row[60]), # PAifPO: "If yes, was he/she allowed to vote?"
			row[61], # PA02Fot: "Did anyone try to photograph the ballot?"
			row[62], # PA03M1F: "Did any voter insert more than one ballot in the box?"
		 	row[63], # PA04PPD: "Unauthorized persons (without badge) stayed in polling station?"
			row[64], # PA05DHU: "There was violence or threats in polling station?"
			row[65], # PA06PRP: "There was political propaganda inside the polling station?"
			row[66], # PA07M1P: "More than 1 person behind the booth (family voting)?"
			row[67], # PA08MBV: "Was the polling station closed at any one point during the day?"
			row[68], # PA09VJK: "Was there voting outside the booths?"
			row[72]  # PA13INC: "Were there any incidents during the election process?"
		]

		return data


	def build_complaints_data(self, row):
		data = [
			row[69], # PA10VAV: "How many voters complained during the voting process"
			row[70], # PA11VMF: "How many voters completed the complaint form?"
			row[71]  # PA12PAA: "If there was a complaint form, were PSC impartial in responding to complaints?"
		]

		return data


	def build_voting_end_data(self, row):
		data = [
			row[73], # PM01PPV: "When did voting process end?"
			row[74], # PM02PJA: "Were there any people waiting in queue when voting center/station closed?"
			row[75], # PM03LVT: "Were they allowed to vote?"
			row[76], # M04NUM: "When did the counting begin?"
			row[89], # PM06PPA: "Were there any unauthorized persons present during counting?"
			row[90], # PM06KUSH: "If yes, who"
			row[91], # PM07VSH: "Could the observer have a clear view of the procedures?"
			row[92], # PM08SHS: "Were the security seals checked before opening the ballot box??"
			row[93], # PM09SS: "Were the security seals intact?"
			row[94], # PM10NEN: "Were the signatures in the voters list counted and recorded?"
			row[96], # PM12NSH: "Number of signature in the voters' list (how many persons voted)?"
			row[97], # PM13FVP: "Were the unused ballots counted and registered?"
			row[99], # PM15VSS: "Were the security seals verified and registered?"
			row[100] # PM16ANA: "Were the voting materials moved aside (unused/damaged/refused ballots, stamps, voters' list, markers, pencils)?"
		]

		return data


	def build_counting_ballots_data(self, row):
		# This data does not exist for 2013 elections
		data = []	
		return data


	def build_counting_summary_data(self, row):
		data = [
			row[112], # PNR01: "Was the decision on dubious ballots just?"
			row[113], # PNR02: "In case of disagreement, were they written in the book?"
			row[114], # PNR03: "When did the counting finish?"
			row[115], # PNR04: "Did any representative or observer oppose the results for this polling station?"
			row[116], # PNR04Kush: "If yes, who"
			row[117]  # PNR05Kom: "Other comments: (If there is a discrepancy between the number of signatures and ballots in the box, explain why)?"
		]

		return data


	def build_results_object(self, row):
		# BALLOTS - MUNICIPAL ASSEMBLY ELECTIONS
		total_ballots_mae = row[101] # PAK01
		invalid_ballots_in_box_mae = row[102] # PAK02
		ballots_set_aside_mae = row[103] # PAK03
		after_counting_did_they_put_votes_in_the_bag = row[104] # PAK04
		
		# BALLOTS - MAYOR ELECTIONS
		total_ballots_me = row[105] # PKK01
		invalid_ballots_in_box_me = row[106] # PKK02
		ballots_set_aside_me = row[107] # PKK03
		bollots_put_in_transaparent_bag = row[108] # PKK04
		condition_ballots = row[109]	#VK00
		number_of_signitures_in_condtion_voting_list = row[110]	#VK01
		did_they_count_envelopes_separatly = row[111]	#VK02
	
		results = {
			'ballots':{
				'municipalAssembly':{
					'total': self.utils.to_num(total_ballots_mae),
					'invalid':{
						'inBallotBox': self.utils.to_num(invalid_ballots_in_box_mae),
						'setAside': self.utils.to_boolean(ballots_set_aside_mae)
					}
				},
				'putInBag': self.utils.to_boolean(after_counting_did_they_put_votes_in_the_bag)
			},
			'mayoral':{
				'ballots':{
					'total': self.utils.to_num(total_ballots_me),
					'invalid':{
						'inBallotBox': self.utils.to_num(invalid_ballots_in_box_me),
						'setAside': self.utils.to_boolean(ballots_set_aside_me)
					},
					'putInTransparentBag': self.utils.to_boolean(bollots_put_in_transaparent_bag),
					'conditionalBallots': self.utils.to_num(condition_ballots),
					'numberOfSignaturesInConditionalVotersList': self.utils.to_num(number_of_signitures_in_condtion_voting_list),
					'envelopesCountedSeparatly': self.utils.to_boolean(did_they_count_envelopes_separatly)
				}
			}
		}

		return results

