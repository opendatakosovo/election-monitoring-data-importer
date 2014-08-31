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
				

				# VOTING MATERIAL
				material_left_behind = row[7] # Column name: 01gja
				have_physical_access = row[8] # Column name: 02gja
			
				

				number_of_accepted_ballots = row[24] # column name:P06NFP
				number_of_voters_in_voting_station_list = row[25] # column name: P07VNL
				number_of_voting_cabins=row[26] # column name:P08NKV
				votingbox_shown_empty=row[27] # column name:P09TKZ
				closed_with_safetystrip = row[28] # column name:P10SHS
				did_they_register_serial_number_of_strips = row[29] # column name:P11NRS
				cabins_provided_voters_safety_and_privancy = row[30] # column name:P12KFV


				# TIME OF COUNTING PROCESS	
				when_voting_process_finished = row[73] #Column name PM01PPV
				anyone_waiting_when_polling_station_closed = row[74] #column name PM02PJA
				did_they_allow_them_to_vote = row[75] #column name PM03LVT
				when_counting_process_started = row[76] #column name M04NUM
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
				any_unauthorized_person_while_counting = row[89] #column name PM06PPA
				who_were_these_unauthorized_persons = row[90] #column name PM06KUSH
				did_they_have_nice_view_in_procedures = row[91] #column name PM07VSH
				did_they_control_safety_strip_before_opening_box = row[92] #column name PM08SHS
				safety_strips_untouched = row[93] #column name PM09SS
				did_they_count_and_register_signitures_in_voters_list = row[94] #column name PM10NEN
				whats_number_of_voters_in_that_polling_station = row[95] #column name PM11NVL
				number_of_signatures_in_voters_list = row[96] #column name PM12NSH
				did_they_count_and_register_unused_ballots = row[97] #column name PM13FVP
				did_they_count_and_register_used_ballots = row[98] #column name PM14PSH
				did_they_verify_and_register_safety_strips = row[99] #column name PM15VSS
				voting_materials_set_aside = row[100] #column name PM16ANA
			
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
			
				# Counting process summary		
				right_decision_for_doubtful_ballots = row[112]	#PNR01
				are_the_disagreements_recorded_in_the_book = row[113] #PNR02
				when_counting_process_finished = row[114] #PNR03
				was_anyone_against_the_results = row[115] #PNR04
				who_was_against_results = row[116]	#PNR04Kush
				other_comments = row[117] #PNR05Kom
				additional_comments = row[134]	#KomShtese

				# TODO: Figure out if invalid_ballots_in_box_xxx and ballots_set_aside_xxx are redundant.
				# If invalid_ballots_in_box_xxx and ballots_set_aside_xxx refer to the same thing then we only need to count (invalid_ballots_in_box_xxx) and not the flag (ballots_set_aside_xxx)


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


				observation = {
					'_id': str(ObjectId()),
					'pollingStation': polling_station,
					'onArrival':{
						'materialLeftBehind': self.utils.to_boolean(material_left_behind),
						'havePhysicalAccess': self.utils.to_boolean(have_physical_access)
					},
					'preparation': preparation,
					'numberOfAcceptedBallots': self.utils.to_num(number_of_accepted_ballots), 
					'numberOfVotersInVotingStationList':self.utils.to_num(number_of_voters_in_voting_station_list),
					'numberOfVotingCabins':self.utils.to_num(number_of_voting_cabins),
					'votingBoxShownAsEmpty': self.utils.to_boolean(votingbox_shown_empty),
					'closedWithSafetyStrip':self.utils.to_boolean( closed_with_safetystrip), 
					'registeredStrips': self.utils.to_boolean(did_they_register_serial_number_of_strips), 
					'cabinsSafetyAndPrivacy': self.utils.to_boolean(cabins_provided_voters_safety_and_privancy),
					'process':{
						'voting': voting_process,
						'counting': {	
							'whenVotingProcessFinished':when_voting_process_finished,
							'anyoneWaitingWhenPollingStationClosed': self.utils.to_boolean(anyone_waiting_when_polling_station_closed),
							'didTheyAllowThemToVote': self.utils.to_boolean(did_they_allow_them_to_vote),
							'whenCountingProcessStarted':when_counting_process_started,
							'observers':{
								'pdk': self.utils.to_boolean(pdk_observers),	
								'ldk': self.utils.to_boolean(ldk_observers), 
								'lvv': self.utils.to_boolean(lvv_observers),
								'aak': self.utils.to_boolean(aak_observers),
								'akr': self.utils.to_boolean(akr_observers),
								'othersParties': other_parties,
								'ngo': self.utils.to_boolean(ngo_observers),
								'media': self.utils.to_boolean(media_observers),
								'international': self.utils.to_boolean(international_observers),
								'other': other_observers
							},
							'unauthorizedPersons':{
								'present': self.utils.to_boolean(any_unauthorized_person_while_counting),
								'who': who_were_these_unauthorized_persons
							},
							'didTheyHaveNiceViewInProcedures': self.utils.to_boolean(did_they_have_nice_view_in_procedures), #FIXME: Who is they? Put they in their own obect.
							'didTheyControlSafetyStripBeforeOpeningBox': self.utils.to_boolean(did_they_control_safety_strip_before_opening_box),#FIXME: Who is they? 
							'safetyStripsUntouched':self.utils.to_boolean(safety_strips_untouched),
							'didTheyCountAndRegisterSignaturesInVotersList': self.utils.to_boolean(did_they_count_and_register_signitures_in_voters_list), #FIXME: Who is they? 
							'whatIsTheNumberOfVotersInPollingStation': self.utils.to_num(whats_number_of_voters_in_that_polling_station),
							'numberOfSignaturesInVotersList': self.utils.to_num(number_of_signatures_in_voters_list),
							'didTheyCountAndRegisterUnusedBallots': self.utils.to_boolean(did_they_count_and_register_unused_ballots), #FIXME: Who is they? 
							'didTheyCountAndRegisterUsedBallots': self.utils.to_boolean(did_they_count_and_register_used_ballots), #FIXME: Who is they? 
							'didTheyVerifyAndRegisterSafetyStrip': self.utils.to_boolean(did_they_verify_and_register_safety_strips), #FIXME: Who is they? 
							'votingMaterialsSetAside': self.utils.to_boolean(voting_materials_set_aside)
						}
					},
					'irregularities': irregularities,				
					'complaints': complaints,
					'ballots':{
						'municipalAssembly':{
							'total': self.utils.to_num(total_ballots_mae),
							'invalid':{
								'inBallotBox': self.utils.to_num(invalid_ballots_in_box_mae),
								'setAside': self.utils.to_boolean(ballots_set_aside_mae)
							},
						'didTheyPutVotesInTheBag': self.utils.to_boolean(after_counting_did_they_put_votes_in_the_bag)
						},
						'mayoral':{
							'total': self.utils.to_num(total_ballots_me),
							'invalid':{
								'inBallotBox': self.utils.to_num(invalid_ballots_in_box_me),
								'setAside': self.utils.to_boolean(ballots_set_aside_me)
							},
							'putInTransparentBag': self.utils.to_boolean(bollots_put_in_transaparent_bag),
							'conditionBallots': self.utils.to_num(condition_ballots),
							'numberOfSignaturesInConditionVotingList': self.utils.to_num(number_of_signitures_in_condtion_voting_list),
							'didTheyCountEnvelopesSeparatly': self.utils.to_boolean(did_they_count_envelopes_separatly)
						}
					},
					'countingProcessSummary': {
						'doubtfulBallotsProperlyHandled': self.utils.translate_frequency(right_decision_for_doubtful_ballots),
						'disgreementsRecorded': self.utils.translate_frequency(are_the_disagreements_recorded_in_the_book),
						'countingProcessFinishTime':when_counting_process_finished,
						'oppositions':{
							'anyoneOpposed': self.utils.to_boolean(was_anyone_against_the_results),
							'who': who_was_against_results
						},
						'comments':other_comments
					}
				} 
			
				# Insert document
				self.mongo.kdi[self.collection_name].insert(observation)
				num_of_created_docs = num_of_created_docs + 1

		return num_of_created_docs


	def build_polling_station_data(self, row):
		data = [
			row[1], #column name EmriV
			row[2], #colun name NrV
			row[3], # Column name nrQV
			row[4], # Column name NRVV
			row[5], # Column name Komuna
			row[6], # Column name EQV
		]

		return data


	def build_preparation_data(self, row):
		data = [
			row[9],  # column name: P01KA
			row[10], # column name: P02A
			row[11], # column name: P02B
			row[12], # column name: P03Perg
			row[13], # column name: P04KVV
			row[14]  # column name: P04Fem
		]

		return data;


	def build_missing_materials_data(self, row):
		data = [
			row[15], # Column name P05Lla
			row[16], # Column name P05Ngj
			row[17], # Column name P05Lis
			row[18], # Column name P05Flv
			row[19], # Column name P05Vul
			row[20], # Column name P05Kut
			row[21], # Column name P05Lib
			row[22], # Column name P05Kab
			row[23]  # Column name P05ZFK 
		]

		return data;


	def build_voting_process_data(self, row):
		voting_process = [
			row[31], # Column name: PV1KHV
			row[44], # Column name: PV03UVL
			row[45], # Column name: PV04IDK
			row[46], # Column name: PV05GSH
			row[47], # Column name: PV06VUL
			row[48], # Column name: PV07-10
			row[49], # Column name: PV07-13
			row[50], # Column name: PV07-16
			row[51], # Column name: PV07-19
			row[52], # Column name: PV08ELV
			row[53], # Column name: PV09NVK
			row[54], # Column name: PV10VAS
			row[55], # Column name: PV11-3AN
			row[58]  # Column name: ProcVotKom
		]

		return voting_process


	def build_refused_ballots_data(self, row):
		data = [
			row[56], # Column name: PV12_Ref
			row[57]  # Column name: PV12IFPo
		]

		return data


	def build_observers_data(self, row):
		data = [
			'PDK' if self.utils.to_boolean(row[32]) else None,  # Column name: PDK
			'LDK' if self.utils.to_boolean(row[33]) else None, # Column name LDK
			'LVV' if self.utils.to_boolean(row[34]) else None, # Column name LVV
			'AAK' if self.utils.to_boolean(row[35]) else None, # Column name AAK
			'AKR' if self.utils.to_boolean(row[36]) else None,  # Column name AKR
			row[37].upper() if row[37] != None else None, # ParTj01
			row[38].upper() if row[38] != None else None, # ParTj02
			row[39].upper() if row[39] != None else None, # ParTj03
			'NGO' if self.utils.to_boolean(row[40]) else None, # OJQ
			'Media' if self.utils.to_boolean(row[41]) else None, # Media
			'International Observers' if self.utils.to_boolean(row[42]) else None  # VzhND
		]

 		# How to handle Others?
		row[43] # VzhTjere

		# e.g. observation with other parties:
		# db.localelectionsfirstround2013.findOne({'pollingStation.number':'1804E'})

		# e.g. others:
		# db.localelectionsfirstround2013.findOne({'pollingStation.number':'1837E'})

		return filter(None, data)


	def build_irregularities_data(self, row):
		data = [
			self.utils.to_boolean(row[59]), #Column name: PA01x1
			self.utils.to_boolean(row[60]), #Column name: PAifPO
			row[61], #Column name: PA02Fot
			row[62], #Column name: PA03M1F
		 	row[63], #Column name: PA04PPD
			row[64], #Column name: PA05DHU
			row[65], #Column name: PA06PRP
			row[66], #Column name: PA07M1P
			row[67], #Column name: PA08MBV
			row[68], #Column name: PA09VJK
			row[72]  #Column name: PA13INC
		]

		return data


	def build_complaints_data(self, row):
		data = [
			row[69], # Column name: PA10VAV
			row[70], # Column name: PA11VMF
			row[71]  #column name PA12PAA
		]

		return data
