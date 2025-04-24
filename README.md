# Tract Acquisition Matching Tool

## Overview
This tool helps identify matching tracts between two datasets using fuzzy matching on metadata fields. It was developed to assist a client with identifying tracts they acquired based on client-provided data and internal database records.

The project leverages the following technologies:
- `pyodbc` for database connections
- `pandas` for data manipulation
- `recordlinkage` for fuzzy matching using `Jaro-Winkler`

## How It Works
1. Connects to a SQL Server database using Azure AD authentication
2. Executes two queries:
   - One against a temporary client-specific table
   - One against the permanent master data
3. Cleans and normalizes key identifying fields (e.g., `FirstParty`, `Book`, `Page`, `Entry`)
4. Uses fuzzy string matching and blocking logic on township/range/grid data to find likely matches
5. Scores matches and selects the best match for each row in the temporary dataset
6. Outputs a final matched CSV file

## Usage
Run the script:
```bash
python similarity.py
```

## Output
The script will generate:
- `final_matched_records.csv`: A CSV file showing all matches with similarity scores and matched field values

## File Structure
```
.
├── match_tracts.py               # Main Python script
├── .env.example                 # Sample environment file
├── requirements.txt            # Dependency list
├── final_matched_records.csv   # Example output file (generated)
└── README.md                   # This file
```

## Example Output
See `final_matched_records.csv` for a sample of what the final output looks like.