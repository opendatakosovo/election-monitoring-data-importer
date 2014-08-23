import csv
from pymongo import MongoClient
from bson import ObjectId
from utils import Utils
from slugify import slugify
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--csvFileName',type=str,help='Type the file name of the CSV that you want data to import in.')
parser.add_argument('--collectionName',type=str, help='Type the collection name[mongodb] that you want data to be in.')

# Parse arguemnts and run the app.	
args = parser.parse_args()

csv_filename = '%s'%(args.csvFileName)
collection_name = '%s'%(args.collectionName)
print csv_filename
# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.kdi
collection = db[collection_name]


# Clear data
collection.remove({})

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

			# OBSERVER AND POLLING STATION INFORMATION
			observer_name = row[1] #column name: EmriV
			observer_number = row[2] #colun name: NrV
			polling_station_number = row[3].lower() # Column name: nrQV
			room_number = row[4] # Column name: NRVV
			
			commune = row[5] # Column name: Komuna
			polling_station_name = row[6] # Column name: EQV

			# VOTING MATERIAL
			material_left_behind = row[7] # Column name: 01gja
			have_physical_access = row[8] # Column name: 02gja
			
			
			# ARRIVAL TIME
			arrival_time = row[9] # column name: P01KA
			how_to_vote_info = row[10] # column name: P02A
			list_of_candidates = row[11] # column name: P02B
			when_preparation_start = row[12] # column name:P03Perg
			number_KVV_members = row[13] #column name:P04KVV
			female = row[14] #column name:P04Fem
			UV_lamp = row[15] #column name:P05Lla
			spray = row[16] # column name:P05Ngj
			voters_list = row[17] # column name:P05Lis
			ballots = row[18] # column name:P05Flv	
			stamp = row[19] # column name:P05Vul
			ballot_box = row[20] # column name:P05Kut
			voters_book = row[21] # column name:P05Lib
			voting_cabin = row[22] # column name:P05Kab
			envelops_condition_voters = row[23] # column name:P05ZFK
			number_of_accepted_ballots = row[24] # column name:P06NFP
			number_of_voters_in_voting_station_list = row[25] # column name: P07VNL
			number_of_voting_cabins=row[26] # column name:P08NKV
			votingbox_shown_empty=row[27] # column name:P09TKZ
			closed_with_safetystrip = row[28] # column name:P10SHS
			did_they_register_serial_number_of_strips = row[29] # column name:P11NRS
			cabins_provided_voters_safety_and_privancy = row[30] # column name:P12KFV

	
			# VOTING PROCESS
			when_voting_process_started = row[31] #Column name: PV1KHV
			pdk_observers_present = row[32] #column name: PDK	
			ldk_observers_present = row[33] #column name LDK
			lvv_observers_present = row[34] #column name LVV	
			aak_observers_present = row[35] #column name AAK
			akr_observers_present = row[36] #column name AKR


			other_parties_observers_1_present = row[37] #column name: ParTj01
			other_parties_observers_2_present = row[38] #column name: ParTj02
			other_parties_observers_3_present = row[39] #column name: ParTj03

			other_parties_observers = [other_parties_observers_1_present, other_parties_observers_2_present, other_parties_observers_3_present]
			other_parties_observers = filter(None, other_parties_observers)

			ngo_observers_present = row[40] #column name: OJQ
			media_observers_present = row[41] #column name: Media
			international_observers_present = row[42] #column name: VzhND
			other_observers_present = row[43] #column name: VzhTjere
			
			# VOTERS INFORMATION
			ultra_violet_control = row[44] # Column name: PV03UVL
			identified_with_document = row[45] # Column name: PV04IDK
			finger_sprayed = row[46] # Column name: PV05GSH
			sealed_ballot = row[47] # Column name: PV06VUL
			how_many_voted_by_ten_AM = row[48] # Column name: PV07-10
			how_many_voted_by_one_PM = row[49] # Column name: PV07-13
			how_many_voted_by_four_PM = row[50] # Column name: PV07-16
			how_many_voted_by_seven_PM = row[51] # Column name: PV07-19
			number_of_voters_who_werent_in_the_voters_list = row[52] #column name:PV08ELV
			number_of_conditional_voters = row[53] #column name: PV09NVK
			number_of_assisted_voters = row[54] #column name: PV10VAS
			at_least_three_kvv_members_present_in_polling_station = row[55] #column name: PV11-3AN
			did_anyone_refused_the_ballot = row[56] #column name: PV12_Ref
			how_many_refused_it = row[57] #column name: PV12IFPo
			voting_process_comments = row[58] # column name: ProcVotKom
			
			# IRREGULARITY AND COMPLAINTS
			attempt_to_vote_more_than_once = row[59] #Column name: PA01x1
			allowed_to_vote = row[60] #Column name: PAifPO
			take_picture_ofballot = row[61] #Column name: PA02Fot
			inserted_more_than_one_ballot_in_the_box = row[62] #Column name: PA03M1F
			unauthorized_persons_stayed_at_the_voting_station = row[63] #Column name: PA04PPD
			violence_in_the_voting_station = row[64] #Column name: PA05DHU
			politic_propaganda_inside_the_voting_station = row[65] #Column name: PA06PRP
			more_than_one_person_behind_the_cabin = row[66] #Column name: PA07M1P
			has_the_voting_station_been_closed_in_any_case = row[67] #Column name: PA08MBV
			case_voting_outside_the_cabin = row[68] #Column name: PA09VJK
			how_many_voters_complained_during_the_process = row[69] #Column name: PA10VAV
			how_many_voters_filled_the_complaints_form = row[70] #Column name: PA11VMF
			are_KVV_members_impartial_when_they_react_to_compliants = row[71] #column name PA12PAA
			any_accident_happened_during_the_process = row[72] #Column name: PA13INC	
			
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
			are_the_disagreements_recorded_in_the_book = row[113]	#PNR02
			when_counting_process_finished = row[114]	#PNR03
			was_anyone_against_the_results = row[115]	#PNR04
			who_was_against_results = row[116]	#PNR04Kush
			other_comments = row[117]	#PNR05Kom
			additional_comments = row[134]	#KomShtese

			# TODO: Figure out if invalid_ballots_in_box_xxx and ballots_set_aside_xxx are redundant.
			# If invalid_ballots_in_box_xxx and ballots_set_aside_xxx refer to the same thing then we only need to count (invalid_ballots_in_box_xxx) and not the flag (ballots_set_aside_xxx)
			
			util = Utils()

			observation = {
				'_id': str(ObjectId()),
				'pollingStation':{
					'observerName' : observer_name,
					'observerNumber' : observer_number,
					'number': polling_station_number,
					'roomNumber': room_number,
					'name': polling_station_name,
					'nameSlug': slugify(polling_station_name),
					'commune': commune,
					'communeSlug': slugify(commune)
				},
				'onArrival':{
					'materialLeftBehind': util.to_boolean(material_left_behind),
					'havePhysicalAccess': util.to_boolean(have_physical_access)
				},
				'preparation':{
					'arrivalTime': arrival_time,
					'votingMaterialsPlacedInAndOutVotingStation':{
						'howToVoteInfo': util.to_boolean(how_to_vote_info),
						'listOfCandidates': util.to_boolean(list_of_candidates), 
						'whenPreparationStarted': when_preparation_start,
						'kvvMembers':{
							'total': util.to_num(number_KVV_members), 
							'female': util.to_num(female) 
						}
					},
				'missingMaterial':{
					'uvLamp': util.to_boolean(UV_lamp), 
					'spray':util.to_boolean( spray), 
					'votersList': util.to_boolean(voters_list),
					'ballots': util.to_boolean(ballots),	
					'stamp': util.to_boolean(stamp),
					'ballotBox':util.to_boolean(ballot_box),
					'votersBook': util.to_boolean(voters_book),
					'votingCabin': util.to_boolean(voting_cabin), 
					'envelopsForConditionVoters': util.to_boolean(envelops_condition_voters),
				},
				'numberOfAcceptedBallots': util.to_num(number_of_accepted_ballots), 
				'numberOfVotersInVotingStationList':util.to_num(number_of_voters_in_voting_station_list),
				'numberOfVotingCabins':util.to_num(number_of_voting_cabins),
				'votingBoxShownAsEmpty': util.to_boolean(votingbox_shown_empty),
				'closedWithSafetyStrip':util.to_boolean( closed_with_safetystrip), 
				'registeredStrips': util.to_boolean(did_they_register_serial_number_of_strips), 
				'cabinsSafetyAndPrivacy': util.to_boolean(cabins_provided_voters_safety_and_privancy),
				},
				'votingProcess':{
					'whenVotingProcessStarted' : when_voting_process_started,
					'observersPresent':{
							'pdk': util.to_boolean(pdk_observers_present),	
							'ldk': util.to_boolean(ldk_observers_present), 
							'lvv': util.to_boolean(lvv_observers_present),
							'aak': util.to_boolean(aak_observers_present),
							'akr': util.to_boolean(akr_observers_present),
							'otherParties' : other_parties_observers,
							'ngo': util.to_boolean(ngo_observers_present),
							'media': util.to_boolean(media_observers_present),
							'international': util.to_boolean(international_observers_present),
							'other': other_observers_present
					},
					'voters':{
						'ultraVioletControl': util.translate_frequency(ultra_violet_control),
						'identifiedWithDocument': util.translate_frequency(identified_with_document),
						'fingerSprayed': util.translate_frequency(finger_sprayed),
						'sealedBallot': util.translate_frequency(sealed_ballot),
						'howManyVotedBy':{
							'tenAM': util.to_num(how_many_voted_by_ten_AM),
							'onePM': util.to_num(how_many_voted_by_one_PM),
							'fourPM': util.to_num(how_many_voted_by_four_PM),
							'sevenPM': util.to_num(how_many_voted_by_seven_PM)
						},
						'howManyVotersWerentInVotersList' : util.to_num(number_of_voters_who_werent_in_the_voters_list),
						'conditionalVoters' : util.to_num(number_of_conditional_voters),
						'numberOfAssistedVoters' : util.to_num(number_of_assisted_voters),
						'anyoneRefusedTheBallot' : util.to_boolean(did_anyone_refused_the_ballot),
						'howMany' : util.to_num(how_many_refused_it)
						
					},
					'atLeastThreeKvvMembersPresentInPollingStation' : util.to_boolean(at_least_three_kvv_members_present_in_polling_station),
					'votingProcessComments' : voting_process_comments
				},
				'irregularities':{
					'attemptToVoteMoreThanOnce':util.to_boolean(attempt_to_vote_more_than_once),
					'allowedToVote':util.to_boolean(allowed_to_vote),
					'photographedBallot':util.to_boolean(take_picture_ofballot),
					'insertedMoreThanOneBallot':util.to_boolean(inserted_more_than_one_ballot_in_the_box),
				 	'unauthorizedPersonsStayedAtTheVotingStation': util.to_boolean(unauthorized_persons_stayed_at_the_voting_station),
					'violenceInTheVotingStation': util.to_boolean(violence_in_the_voting_station),
					'politicalPropagandaInsideTheVotingStation': util.to_boolean(politic_propaganda_inside_the_voting_station),
					'moreThanOnePersonBehindTheCabin': util.to_boolean(more_than_one_person_behind_the_cabin),
					'hasTheVotingStationBeenClosedInAnyCase': util.to_boolean(has_the_voting_station_been_closed_in_any_case),
					'caseVotingOutsideTheCabin': util.to_boolean(case_voting_outside_the_cabin),
					'areTheKvvMembersImpartialWhenTheyReactToComplaints' : util.translate_frequency(are_KVV_members_impartial_when_they_react_to_compliants),
					'anyAccidentHappenedDuringTheProcess': util.to_boolean(any_accident_happened_during_the_process)
				},					
				'complaints':{
					'total': util.to_num(how_many_voters_complained_during_the_process),
					'filled': util.to_num(how_many_voters_filled_the_complaints_form)
				},

				'countingProcess':{	
						'whenVotingProcessFinished':when_voting_process_finished,
						'anyoneWaitingWhenPollingStationClosed': util.to_boolean(anyone_waiting_when_polling_station_closed),
						'didTheyAllowThemToVote': util.to_boolean(did_they_allow_them_to_vote),
						'whenCountingProcessStarted':when_counting_process_started,
						'observers':{
							'pdk': util.to_boolean(pdk_observers),	
							'ldk': util.to_boolean(ldk_observers), 
							'lvv': util.to_boolean(lvv_observers),
							'aak': util.to_boolean(aak_observers),
							'akr': util.to_boolean(akr_observers),
							'othersParties': other_parties,
							'ngo': util.to_boolean(ngo_observers),
							'media': util.to_boolean(media_observers),
							'international': util.to_boolean(international_observers),
							'other': other_observers
						},
						'unauthorizedPersons':{
							'present': util.to_boolean(any_unauthorized_person_while_counting),
							'who': who_were_these_unauthorized_persons
						},
						'didTheyHaveNiceViewInProcedures': util.to_boolean(did_they_have_nice_view_in_procedures), #FIXME: Who is they? Put they in their own obect.
						'didTheyControlSafetyStripBeforeOpeningBox': util.to_boolean(did_they_control_safety_strip_before_opening_box),#FIXME: Who is they? 
						'safetyStripsUntouched':util.to_boolean(safety_strips_untouched),
						'didTheyCountAndRegisterSignaturesInVotersList': util.to_boolean(did_they_count_and_register_signitures_in_voters_list), #FIXME: Who is they? 
						'whatIsTheNumberOfVotersInPollingStation': util.to_num(whats_number_of_voters_in_that_polling_station),
						'numberOfSignaturesInVotersList': util.to_num(number_of_signatures_in_voters_list),
						'didTheyCountAndRegisterUnusedBallots': util.to_boolean(did_they_count_and_register_unused_ballots), #FIXME: Who is they? 
						'didTheyCountAndRegisterUsedBallots': util.to_boolean(did_they_count_and_register_used_ballots), #FIXME: Who is they? 
						'didTheyVerifyAndRegisterSafetyStrip': util.to_boolean(did_they_verify_and_register_safety_strips), #FIXME: Who is they? 
						'votingMaterialsSetAside': util.to_boolean(voting_materials_set_aside)
					},
				'ballots':{
					'municipalAssembly':{
						'total': util.to_num(total_ballots_mae),
						'invalid':{
							'inBallotBox': util.to_num(invalid_ballots_in_box_mae),
							'setAside': util.to_boolean(ballots_set_aside_mae)
						},
					'didTheyPutVotesInTheBag': util.to_boolean(after_counting_did_they_put_votes_in_the_bag)
					},
					'mayoral':{
						'total': util.to_num(total_ballots_me),
						'invalid':{
							'inBallotBox': util.to_num(invalid_ballots_in_box_me),
							'setAside': util.to_boolean(ballots_set_aside_me)
						},
						'putInTransparentBag': util.to_boolean(bollots_put_in_transaparent_bag),
						'conditionBallots': util.to_num(condition_ballots),
						'numberOfSignaturesInConditionVotingList': util.to_num(number_of_signitures_in_condtion_voting_list),
						'didTheyCountEnvelopesSeparatly': util.to_boolean(did_they_count_envelopes_separatly)
					}
				},
				'countingProcessSummary': {
					'doubtfulBallotsProperlyHandled': util.translate_frequency(right_decision_for_doubtful_ballots),
					'disgreementsRecorded': util.translate_frequency(are_the_disagreements_recorded_in_the_book),
					'countingProcessFinishTime':when_counting_process_finished,
					'oppositions':{
						'anyoneOpposed': util.to_boolean(was_anyone_against_the_results),
						'who': who_was_against_results
					},
					'comments':other_comments
				}
			} 
			
			# Insert document
			collection.insert(observation)

parse_csv()	
