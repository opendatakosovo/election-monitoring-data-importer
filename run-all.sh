source ./venv/bin/activate
python run_importer.py --organization=dia --electionYear=2013 --electionType=local-elections --electionRound=first-round
python run_importer.py --organization=dia --electionYear=2013 --electionType=local-elections --electionRound=second-round
python run_importer.py --organization=dia --electionYear=2014 --electionType=general-elections --electionRound=first-round
