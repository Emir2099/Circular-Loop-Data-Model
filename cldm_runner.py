import os
import re
import sys
# import matplotlib.pyplot as plt
import bson
from custom_database import CustomDatabase 
import time
# import mplcursors
from filelock import FileLock

# Define regex patterns for each command type
patterns = {
    'create_loop': r'^FORGE LOOP (\w+)$',
    'segment_loop': r'^SEGMENT LOOP (\w+) INTO (\w+)$',
    'craft': r'^CRAFT "([^"]+)" INTO (\w+) IN (\w+)$',
    'change_master': r'^CHANGE MASTER SEGMENT OF (\w+) TO (\w+)$',
    'link_segment': r'^LINK SEGMENT (\w+) IN (\w+) TO (\w+) IN (\w+)$',
    'visualize': r'^VISUALIZE LOOP (\w+)$',
    'destroy_database': r'^DESTROY DATABASE$',

    # New custom retrieval commands
    'basic_retrieval': r'^EXTRACT ENTRIES OF ([\w\s,]+) WITHIN (\w+)$',
    'conditional_retrieval': r'^EXTRACT ENTRIES OF ([\w\s,]+) WITHIN (\w+) FILTER BY (\w+) MATCHES "([^"]+)"$',
    'pattern_based_retrieval': r'^EXTRACT ENTRIES OF ([\w\s,]+) WITHIN (\w+) FILTER BY (\w+) RESEMBLES "([^"]+)"$',
    'ordered_retrieval': r'^EXTRACT ENTRIES OF ([\w\s,]+) WITHIN (\w+) SORTED AS (\w+) (ASCENDING|DESCENDING)$',
    'limiting_retrieval': r'^EXTRACT ENTRIES OF ([\w\s,]+) WITHIN (\w+) LIMIT TO (\d+) ENTRIES$',
    'combined_conditions_retrieval': r'^EXTRACT ENTRIES OF ([\w\s,]+) WITHIN (\w+) FILTER BY (\w+) MATCHES "([^"]+)" AND (\w+) MATCHES "([^"]+)"$',
    'unique_entry_retrieval': r'^EXTRACT DISTINCT ENTRIES OF ([\w\s,]+) WITHIN (\w+)$',
    'counting_entries': r'^TALLY ENTRIES IN (\w+) WITHIN (\w+)$',
    'aggregating_data': r'^COMBINE (SUM|GATHER MAXIMUM|GATHER MINIMUM|COLLECT) VALUES OF (\w+) WITHIN (\w+)(?: FILTER BY (\w+) MATCHES "([^"]+)")?$',
    
    #database creation
    'create_database': r'^CREATE DATABASE (\w+\.bson)$',   # Create new database
    'load_database': r'^LOAD DATABASE (\w+\.bson)$',        # Load existing database
    
    'dismantle_loop': r'^DISMANTLE LOOP (\w+)$',  # Dismantle loop pattern
    'remove_segment': r'^REMOVE SEGMENT (\w+) FROM LOOP (\w+)$',  # Remove segment from loop pattern
    'remove_data': r'^REMOVE "([^"]+)" FROM SEGMENT (\w+) IN LOOP (\w+)$'  # Remove data from segment pattern
}


        
def parse_command(command):
    print(f"\033[93mParsing command: {command}\033[0m")  # Yellow for debug statements
    for action, pattern in patterns.items():
        print(f"\033[93mTrying pattern for action '{action}': {pattern}\033[0m")  # Yellow for debug statements
        match = re.match(pattern, command, re.IGNORECASE)
        if match:
            print(f"\033[93mPattern matched for action '{action}'\033[0m")  # Yellow for debug statements
            if action == 'create_database':
                return {'action': 'create_database', 'database_name': match.group(1)}
            # Load Database Command
            elif action == 'load_database':
                return {
                    'action': 'load_database',
                    'database_name': match.group(1)
                }
            elif action == 'create_loop':
                return {
                    'action': 'create_loop',
                    'loop_name': match.group(1)
                }
            elif action == 'segment_loop':
                return {
                    'action': 'segment_loop',
                    'loop_name': match.group(1),
                    'segment_name': match.group(2)
                }
            elif action == 'craft':
                return {
                    'action': 'craft',
                    'value': match.group(1),
                    'segment_name': match.group(2),
                    'loop_name': match.group(3)
                }
            elif action == 'select':
                return {
                    'action': 'select',
                    'attributes': match.group(1).strip().split(','),
                    'loop_name': match.group(2),
                    'condition': match.group(3)
                }
            elif action == 'change_master':
                return {
                    'action': 'change_master',
                    'loop_name': match.group(1),
                    'segment_name': match.group(2)
                }
            elif action == 'link_segment':
                 return {
                    'action': 'link_segment',
                    'segment_name': match.group(1),
                    'loop_name': match.group(2),
                    'foreign_segment': match.group(3),
                    'foreign_loop': match.group(4)
                }
                
            elif action == 'visualize':
                return {'action': 'visualize', 'loop_name': match.group(1)}  # Use group(1) here
            
            elif action == 'destroy_database':
                return {'action': 'destroy_database'}

            elif action == 'dismantle_loop':
                return {'action': 'dismantle_loop', 'loop_name': match.group(1)}
            elif action == 'remove_segment':
                return {'action': 'remove_segment', 'segment_name': match.group(1), 'loop_name': match.group(2)}
            elif action == 'remove_data':
                return {'action': 'remove_data', 'data': match.group(1), 'segment_name': match.group(2), 'loop_name': match.group(3)}
            
            # Basic retrieval
            if action == 'basic_retrieval':
                return {
                    'action': 'basic_retrieval',
                    'segments': match.group(1).strip().split(','),
                    'loop_name': match.group(2)
                }
            # Conditional retrieval
            elif action == 'conditional_retrieval':
                return {
                        'action': 'conditional_retrieval',
                        'segments': match.group(1).strip().split(','),
                        'loop_name': match.group(2),
                        'filter_segment': match.group(3).strip(),
                        'filter_value': match.group(4).strip('"')
                        }

            # Pattern-based retrieval
            elif action == 'pattern_based_retrieval':
                return {
                    'action': 'pattern_based_retrieval',
                    'segments': match.group(1).strip().split(','),
                    'loop_name': match.group(2),
                    'filter_segment': match.group(3),
                    'pattern': match.group(4)
                }
            # Ordered retrieval
            elif action == 'ordered_retrieval':
                return {
                    'action': 'ordered_retrieval',
                    'segments': match.group(1).strip().split(','),
                    'loop_name': match.group(2),
                    'sort_segment': match.group(3),
                    'order': match.group(4)
                }
            # Limiting retrieval
            elif action == 'limiting_retrieval':
                return {
                    'action': 'limiting_retrieval',
                    'segments': match.group(1).strip().split(','),
                    'loop_name': match.group(2),
                    'limit': int(match.group(3))
                }
            # Combined conditions retrieval
            elif action == 'combined_conditions_retrieval':
                return {
                    'action': 'combined_conditions_retrieval',
                    'segments': match.group(1).strip().split(','),
                    'loop_name': match.group(2),
                    'filter_segment1': match.group(3),
                    'filter_value1': match.group(4),
                    'filter_segment2': match.group(5),
                    'filter_value2': match.group(6)
                }
            # Unique entry retrieval
            elif action == 'unique_entry_retrieval':
                return {
                    'action': 'unique_entry_retrieval',
                    'segments': match.group(1).strip().split(','),
                    'loop_name': match.group(2)
                }
            # Counting entries
            elif action == 'counting_entries':
                return {
                    'action': 'counting_entries',
                    'segment': match.group(1),
                    'loop_name': match.group(2)
                }
            # Aggregating data
            elif action == 'aggregating_data':
                return {
                    'action': 'aggregating_data',
                    'aggregation_type': match.group(1),
                    'segment_name': match.group(2),
                    'loop_name': match.group(3),
                    'filter_segment': match.group(4),
                    'filter_value': match.group(5)
                }


    print("\033[93mNo matching pattern found.\033[0m")  # Yellow for debug statements
    return None




def execute_command(parsed_command, database):# Create Database Command
    global database_initialized  # To track if the database is loaded or created

    # Create Database Command
    if parsed_command['action'] == 'create_database':
        db_file_path = parsed_command['database_name']
    
    # Check if the database file already exists
        if os.path.exists(db_file_path):
            print(f"\033[91mError: Database '{db_file_path}' already exists.\033[0m")
        else:
            if database is None:  # Initialize the database if not already created/loaded
                database = CustomDatabase(db_file_path)
            database.set_database(db_file_path)
            database.save()  # Save the newly created database immediately
            print(f"\033[92mDatabase '{db_file_path}' created and loaded for use.\033[0m")
            return database

    # Load Database Command
    elif parsed_command['action'] == 'load_database':
        db_file_path = parsed_command['database_name']

        # Check if the database file exists before loading
        if not os.path.exists(db_file_path):
            print(f"\033[91mError: Database '{db_file_path}' does not exist.\033[0m")
            return database  # Return without loading anything

        # If the database file exists, proceed with loading it
        if database is None:  # Initialize the database if not loaded
            database = CustomDatabase(db_file_path)
        database.set_database(db_file_path)
        print(f"\033[92mLoaded database: {db_file_path}\033[0m")
        return database

    # Other commands require a database to be loaded first
    elif parsed_command['action'] in [ 'forge','create_loop','segment_loop','craft','change_master','link_segment','dismantle_loop','remove_segment','remove_data','visualize','destroy_database','basic_retrieval','conditional_retrieval','pattern_based_retrieval','ordered_retrieval','limiting_retrieval','combined_conditions_retrieval','unique_entry_retrieval','counting_entries','aggregating_data']:
        if database is None:  # No database loaded, show an error
            print("\033[91mError: No database loaded. Load or create a database first.\033[0m")
            return None  # Exit the function without execution
        else:
    
            print(f"\033[93mExecuting command: {parsed_command}\033[0m")  # Yellow for debug statements
            db_data = database.data
    # if db_data is None:
    #     print("\033[91mDatabase is not loaded properly.\033[0m")  # Red for error messages
    #     return
    
        try:
            if parsed_command['action'] == 'create_database':
                db_file_path = parsed_command['database_name']
                if not os.path.exists(db_file_path):
                    open(db_file_path, 'wb').close()  # Create an empty BSON file
                    print(f"\033[92mDatabase {db_file_path} created.\033[0m")
                else:
                    print(f"\033[91mError: Database '{db_file_path}' already exists.\033[0m")
                return
            
            elif parsed_command['action'] == 'load_database':
                db_file_path = parsed_command['database_name']
                database.set_database(db_file_path)
                print(f"Loaded database: {db_file_path}")
            
            elif parsed_command['action'] == "create_loop":
                loop_name = parsed_command['loop_name']
                if loop_name in db_data:
                    print(f"\033[91mError: Loop '{loop_name}' already exists.\033[0m")  # Red for error messages
                    return database  # Prevent duplicate loop creation
                db_data[loop_name] = {}
                print(f"\033[92mLoop {loop_name} created.\033[0m")  # Green for user-visible text
                database.save()
                return database  # Return after creating loop

            elif parsed_command['action'] == "segment_loop":
                loop_name = parsed_command['loop_name']
                segment_name = parsed_command['segment_name']
                if loop_name in db_data:
                    if segment_name in db_data[loop_name]:
                        print(f"\033[91mError: Segment '{segment_name}' already exists in Loop '{loop_name}'.\033[0m")  # Red for error messages
                        return database  # Prevent duplicate segment creation
                    db_data[loop_name][segment_name] = []
                    print(f"\033[92mSegment {segment_name} added to Loop {loop_name}.\033[0m")  # Green for user-visible text
                    database.save()
                    return database  # Return after adding segment
                else:
                    print(f"\033[91mLoop {loop_name} does not exist.\033[0m")  # Red for error messages
                    return database  # Return even if the loop does not exist

            elif parsed_command['action'] == "craft":
                loop_name = parsed_command['loop_name']
                segment_name = parsed_command['segment_name']
                value = parsed_command['value']
                print(f"\033[93mCrafting value {value} in segment {segment_name} in Loop {loop_name}.\033[0m")  # Debug

                if loop_name in db_data and segment_name in db_data[loop_name]:
                # Check if crafting into the master segment and enforce uniqueness
                    is_master = (segment_name == db_data[loop_name].get('MasterSegment'))
        
                    if is_master and value in db_data[loop_name][segment_name]:
                        print(f"\033[91mDuplicate value '{value}' detected in Master Segment '{segment_name}'. Reassigning master.\033[0m")
                        # Add the value then reassign the master if duplicates exist
                        db_data[loop_name][segment_name].append(value)
                        check_master_segment(db_data[loop_name])  # Reassign master if needed
                    else:
                        # Craft value normally
                        db_data[loop_name][segment_name].append(value)
                        print(f"\033[92mValue '{value}' added to Segment '{segment_name}' in Loop '{loop_name}'.\033[0m")  # Green for user-visible text
        
                    # Save the database state
                    database.save()
                    return database
                else:
                    print(f"\033[91mLoop '{loop_name}' or Segment '{segment_name}' does not exist.\033[0m")  # Error messages
                    return database

            elif parsed_command['action'] == "select":
                loop_name = parsed_command['loop_name']
                condition_key, condition_value = parsed_command['condition'].split('=')
                condition_key = condition_key.strip()
                condition_value = condition_value.strip().strip('"')
                print(f"\033[93mSelecting from loop {loop_name} where {condition_key} = {condition_value}\033[0m")  # Yellow for debug

                if loop_name in db_data:
                    result = []
                    for segment_name, values in db_data[loop_name].items():
                        if segment_name == condition_key:
                            for value in values:
                                if value == condition_value:
                                    result.append({attr: value for attr in parsed_command['attributes']})
                    if result:
                        print(f"\033[92mResult: {result}\033[0m")  # Green for user-visible text
                    else:
                        print(f"\033[91mNo matching records found.\033[0m")  # Red for error messages
                    return database  # Return after selection
                else:
                    print(f"\033[91mLoop {loop_name} does not exist.\033[0m")  # Red for error messages
                    return database

            elif parsed_command['action'] == "link_segment":
                loop_name = parsed_command['loop_name']
                segment_name = parsed_command['segment_name']
                foreign_loop = parsed_command['foreign_loop']
                foreign_segment = parsed_command['foreign_segment']

                if loop_name in db_data and foreign_loop in db_data:
                    if segment_name in db_data[loop_name] and foreign_segment in db_data[foreign_loop]:
                        db_data[loop_name][segment_name] = db_data[foreign_loop][foreign_segment]
                        print(f"\033[92mSegment '{segment_name}' in Loop '{loop_name}' linked to Segment '{foreign_segment}' in Loop '{foreign_loop}'.\033[0m")
                        database.save()
                        return database  # Return after linking
                    else:
                        print(f"\033[91mError: Segment '{segment_name}' or '{foreign_segment}' does not exist.\033[0m")
                        return database  # Return if segment does not exist
                else:
                    print(f"\033[91mError: Loop '{loop_name}' or '{foreign_loop}' does not exist.\033[0m")
                    return database

            # In execute_command function, add this for the destroy_database action
            elif parsed_command['action'] == "destroy_database":
            # Access the file path from the database instance
                try:
                    if hasattr(database, 'db_file_path') and os.path.exists(database.db_file_path):
                        os.remove(database.db_file_path)
                        print(f"\033[92mDatabase '{database.db_file_path}' destroyed successfully.\033[0m")
                        database = None  # Set database to None after destruction
                    else:
                        print(f"\033[91mError: No valid file path found for the loaded database.\033[0m")
                except Exception as e:
                    print(f"\033[91mError destroying database: {str(e)}\033[0m")

                return None



            elif parsed_command['action'] == "visualize":
                loop_name = parsed_command['loop_name']
                if loop_name in db_data:
                    loop_data = db_data[loop_name]

                    # Identify the actual master segment
                    master_segment = loop_data.get('MasterSegment', None)

                    # Print loop details
                    segment_names = [seg for seg in loop_data.keys() if seg != 'MasterSegment']
                    print(f"\033[92mLoop: {loop_name}\033[0m")
                    print(f"\033[94mSegments: {', '.join(segment_names)}\033[0m")  # Display segment names in blue

                    print("\nSegment Entries:")
                    for segment, values in loop_data.items():
                        # Skip the 'MasterSegment' entry itself from being displayed as a segment
                        if segment == 'MasterSegment':
                            continue

                        # Display the segment name with (MS) suffix if it's the master segment
                        segment_display_name = f"{segment} (MS)" if segment == master_segment else segment
                        print(f"\033[96mSegment: {segment_display_name}\033[0m")
                        for value in values:
                            print(f"  - {value}")
                        print("-" * 40)  # Divider for better readability
                    database.save()
                    return database  # Return after visualization
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist in the database.\033[0m")
                    return database

            # Dismantle Loop Command
            elif parsed_command['action'] == 'dismantle_loop':
                loop_name = parsed_command['loop_name']
                if loop_name in database.data:
                    del database.data[loop_name]
                    database.save()
                    print(f"\033[92mLoop '{loop_name}' dismantled.\033[0m")
                    return database
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                    return database

            # Remove Segment from Loop Command
            elif parsed_command['action'] == 'remove_segment':
                segment_name = parsed_command['segment_name']
                loop_name = parsed_command['loop_name']
                if loop_name in database.data and segment_name in database.data[loop_name]:
                    del database.data[loop_name][segment_name]
                    database.save()
                    print(f"\033[92mSegment '{segment_name}' removed from loop '{loop_name}'.\033[0m")
                    return database
                elif loop_name not in database.data:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                    return database
                else:
                    print(f"\033[91mError: Segment '{segment_name}' does not exist in loop '{loop_name}'.\033[0m")
                    return database

            # Remove Data from Segment in Loop Command
            elif parsed_command['action'] == 'remove_data':
                data_to_remove = parsed_command['data']
                segment_name = parsed_command['segment_name']
                loop_name = parsed_command['loop_name']
                if loop_name in database.data and segment_name in database.data[loop_name]:
                    segment_data = database.data[loop_name][segment_name]
                    if data_to_remove in segment_data:
                        segment_data.remove(data_to_remove)
                        database.save()
                        print(f"\033[92mData '{data_to_remove}' removed from segment '{segment_name}' in loop '{loop_name}'.\033[0m")
                        return database  
                    else:
                        print(f"\033[91mError: Data '{data_to_remove}' not found in segment '{segment_name}' in loop '{loop_name}'.\033[0m")
                        return database
                elif loop_name not in database.data:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                    return database
                else:
                    print(f"\033[91mError: Segment '{segment_name}' does not exist in loop '{loop_name}'.\033[0m")
                    return database
    
            elif parsed_command['action'] == "basic_retrieval":
                segments = [seg.strip() for seg in parsed_command['segments']]  # Ensure no leading/trailing spaces
                loop_name = parsed_command['loop_name']

                if loop_name in db_data:
                    results = {segment: db_data[loop_name].get(segment, []) for segment in segments}

                    print(f"Basic Retrieval Results for Loop '{loop_name}':")
                    for segment, values in results.items():
                        if values:
                            print(f"Segment {segment}: {', '.join(values)}")
                        else:
                            print(f"Segment {segment}: No entries found")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                return database

            elif parsed_command['action'] == "conditional_retrieval":
                segments = parsed_command['segments']
                loop_name = parsed_command['loop_name']
                filter_segment = parsed_command['filter_segment']
                filter_value = parsed_command['filter_value']
            
                if loop_name in db_data:
                    filtered_results = {}
                    if filter_segment.strip() in db_data[loop_name]:
                        for segment in segments:
                            segment = segment.strip()  # Remove any leading/trailing whitespaces
                            if segment in db_data[loop_name]:
                                filtered_results[segment] = [
                                    value for i, value in enumerate(db_data[loop_name][segment])
                                    if db_data[loop_name][filter_segment][i] == filter_value
                                ]
            
                    print(f"Conditional Retrieval Results for Loop '{loop_name}':")
                    for segment, values in filtered_results.items():
                        print(f"Segment {segment}: {', '.join(values)}")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                return database

            elif parsed_command['action'] == "pattern_based_retrieval":
                segments = parsed_command['segments']
                loop_name = parsed_command['loop_name']
                filter_segment = parsed_command['filter_segment']
                pattern = parsed_command['pattern']

                if loop_name in db_data:
                    filtered_results = {}

                    if filter_segment.strip() in db_data[loop_name]:
                        regex = re.compile(pattern)  # Compile the regex pattern
                        for segment in segments:
                            segment = segment.strip()  # Clean any leading/trailing spaces
                            if segment in db_data[loop_name]:
                                filtered_results[segment] = [
                                    value for value in db_data[loop_name][segment]
                                    if regex.match(value)
                                ]

                    print(f"Pattern-Based Retrieval Results for Loop '{loop_name}':")
                    for segment, values in filtered_results.items():
                        if values:
                            print(f"Segment {segment}: {', '.join(values)}")
                        else:
                            print(f"Segment {segment}: No entries match the pattern")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                return database
            

            elif parsed_command['action'] == "ordered_retrieval":
                segments = [segment.strip() for segment in parsed_command['segments']]  # Strip whitespace
                loop_name = parsed_command['loop_name']
                sort_segment = parsed_command['sort_segment'].strip()  # Strip whitespace
                order = parsed_command['order']

                if loop_name in db_data and sort_segment in db_data[loop_name]:
                    # Create ordered_results dictionary for the segments
                    ordered_results = {segment: db_data[loop_name].get(segment, []) for segment in segments}

                    # Get sorted indices based on the values in the sort_segment
                    sorted_indices = sorted(
                        range(len(db_data[loop_name][sort_segment])),
                        key=lambda i: db_data[loop_name][sort_segment][i],
                        reverse=(order.lower() == "descending")
                    )

                    print(f"Ordered Retrieval Results for Loop '{loop_name}':")
                    for segment in segments:
                        if segment in ordered_results:
                            # Collect values based on sorted indices
                            sorted_values = [ordered_results[segment][i] for i in sorted_indices if i < len(ordered_results[segment])]
                            print(f"Segment {segment}: {', '.join(sorted_values)}")
                        else:
                            print(f"\033[91mError: Segment '{segment}' does not exist in the loop '{loop_name}'.\033[0m")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' or sorting segment '{sort_segment}' does not exist.\033[0m")
                return database


            elif parsed_command['action'] == "limiting_retrieval":
                segments = parsed_command['segments']
                loop_name = parsed_command['loop_name']
                limit = parsed_command['limit']

                if loop_name in db_data:
                    results = {segment: db_data[loop_name].get(segment, [])[:limit] for segment in segments}
                    print(f"Limiting Retrieval Results for Loop '{loop_name}':")
                    for segment, values in results.items():
                        print(f"Segment {segment}: {', '.join(values)}")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                return database

            elif parsed_command['action'] == "combined_conditions_retrieval":
                segments = [segment.strip() for segment in parsed_command['segments']]  # Strip whitespace
                loop_name = parsed_command['loop_name']
                filter_segment1 = parsed_command['filter_segment1'].strip()
                filter_value1 = parsed_command['filter_value1'].strip()
                filter_segment2 = parsed_command['filter_segment2'].strip()
                filter_value2 = parsed_command['filter_value2'].strip()

                if loop_name in db_data:
                    combined_results = {segment: [] for segment in segments}

                    # Iterate over the range of entries in the loop's segments
                    for i in range(len(db_data[loop_name][filter_segment1])):
                        # Check the combined conditions for both filters
                        if (db_data[loop_name][filter_segment1][i] == filter_value1 and
                            db_data[loop_name][filter_segment2][i] == filter_value2):

                            # Collect the filtered results for the specified segments
                            for segment in segments:
                                if segment in db_data[loop_name]:
                                    combined_results[segment].append(db_data[loop_name][segment][i])

                    # Display the combined results
                    print(f"Combined Conditions Retrieval Results for Loop '{loop_name}':")
                    for segment, values in combined_results.items():
                        print(f"Segment {segment}: {', '.join(values) if values else 'No matching entries'}")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                return database

            elif parsed_command['action'] == "unique_entry_retrieval":
                segments = parsed_command['segments']
                loop_name = parsed_command['loop_name']

                if loop_name in db_data:
                    results = {segment: list(set(db_data[loop_name].get(segment, []))) for segment in segments}
                    print(f"Unique Entry Retrieval Results for Loop '{loop_name}':")
                    for segment, values in results.items():
                        print(f"Segment {segment}: {', '.join(values)}")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")
                return database

            elif parsed_command['action'] == "counting_entries":
                segment = parsed_command['segment']
                loop_name = parsed_command['loop_name']

                if loop_name in db_data and segment in db_data[loop_name]:
                    count = len(db_data[loop_name][segment])
                    print(f"Total entries in segment '{segment}' within loop '{loop_name}': {count}")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' or Segment '{segment}' does not exist.\033[0m")
                return database

            elif parsed_command['action'] == "aggregating_data":
                aggregation_type = parsed_command['aggregation_type'].strip()
                segment_name = parsed_command['segment_name'].strip()
                loop_name = parsed_command['loop_name'].strip()
    
                # Check for filter segment and value in the parsed command
                filter_segment = parsed_command.get('filter_segment', None)
                filter_value = parsed_command.get('filter_value', None)

                if loop_name in db_data and segment_name in db_data[loop_name]:
                    # Collect values based on whether a filter is provided
                    if filter_segment and filter_value:
                        filter_value = filter_value.strip('"')
                        if filter_segment in db_data[loop_name]:
                            segment_values = []
                            for i in range(len(db_data[loop_name][filter_segment])):
                                if db_data[loop_name][filter_segment][i] == filter_value:
                                    try:
                                        # Try converting to float; skip if it fails
                                        segment_values.append(float(db_data[loop_name][segment_name][i]))
                                    except ValueError:
                                        continue
                        else:
                            print(f"\033[91mError: Filter Segment '{filter_segment}' does not exist within '{loop_name}'.\033[0m")
                            return database
                    else:
                        # No filter provided; attempt to collect all float-convertible values
                        segment_values = []
                        for value in db_data[loop_name][segment_name]:
                            try:
                                segment_values.append(float(value))
                            except ValueError:
                                continue

                    # If no float-convertible values are found, return an error
                    if not segment_values:
                        print(f"\033[91mError: No float-convertible values in segment '{segment_name}'.\033[0m")
                        return database

        # Perform the aggregation based on the type
                    if aggregation_type.lower() == "sum":
                        result = sum(segment_values)
                    elif aggregation_type.lower() == "gather maximum":
                        result = max(segment_values) if segment_values else "No matching values"
                    elif aggregation_type.lower() == "gather minimum":
                        result = min(segment_values) if segment_values else "No matching values"
                    elif aggregation_type.lower() == "collect":
                        result = segment_values

                    # Print the result of the aggregation
                    if result is not None:
                         print(f"{aggregation_type} of {segment_name} within '{loop_name}': {result}")
                else:
                    print(f"\033[91mError: Loop '{loop_name}' or Segment '{segment_name}' does not exist.\033[0m")
    
                return database






            elif parsed_command['action'] == "change_master":
                loop_name = parsed_command['loop_name']
                segment_name = parsed_command['segment_name']
                print(f"\033[93mAttempting to change master segment to {segment_name} for loop {loop_name}\033[0m")  # Debug message

                if loop_name in db_data:
                    segment_values = db_data[loop_name].get(segment_name, [])

                    # Check if the selected segment has unique values
                    if len(segment_values) == len(set(segment_values)):
                    # Set the new master segment only if unique
                        db_data[loop_name]['MasterSegment'] = segment_name
                        print(f"\033[92mMaster Segment changed to {segment_name} for Loop {loop_name}.\033[0m")
                        database.save()  # Save changes as the new master has been set
                    else:
                    # Keep the current master segment if it exists
                        current_master = db_data[loop_name].get('MasterSegment', 'None')
                        if current_master == 'None':
                            print(f"\033[91mError: Segment '{segment_name}' contains non-unique values and cannot be used as the Master Segment.\033[0m")
                            print(f"\033[93mNo Master Segment is currently set.\033[0m")
                        else:
                            print(f"\033[91mError: Segment '{segment_name}' contains non-unique values and cannot be used as the Master Segment.\033[0m")
                            print(f"\033[93mCurrent Master Segment remains: {current_master}\033[0m")

                    return database
                else:
                    print(f"\033[91mError: Loop '{loop_name}' does not exist.\033[0m")  # Error for missing loop
                    return database



        
        except IndexError as e:
            print(f"\033[91mError: {str(e)}\033[0m")  # Red for error messages
            print(f"\033[91mFailed to execute command: {parsed_command}\033[0m")  # Red for error messages
            return database




def check_master_segment(loop_data):
    current_master = loop_data.get('MasterSegment', None)
    max_segment_length = 0
    new_master_segment = None

    for segment_name, values in loop_data.items():
        # Skip MasterSegment itself and check for unique values
        if segment_name != 'MasterSegment' and len(values) == len(set(values)):
            if len(values) > max_segment_length:
                new_master_segment = segment_name
                max_segment_length = len(values)

    # Update MasterSegment if a suitable new one is found
    if new_master_segment and new_master_segment != current_master:
        loop_data['MasterSegment'] = new_master_segment
        print(f"\033[92mMaster Segment selected: {new_master_segment}\033[0m")
    elif not new_master_segment and not current_master:
        # Default to a generated master segment if no unique segment is available
        loop_data['MasterSegment'] = list(map(str, range(1, max_segment_length + 1)))
        print(f"\033[92mNew Master Segment created with default unique values: {loop_data['MasterSegment']}\033[0m")
    else:
        # Keep the existing MasterSegment if no suitable replacement was found
        if current_master:
            print(f"\033[93mCurrent Master Segment remains: {current_master}\033[0m")






def execute_file_commands(cldm_file_path):
    # Initialize with no database loaded
    database = None

    with open(cldm_file_path, 'r') as file:
        commands = file.readlines()

    current_block = []

    for command in commands:
        command = command.strip()
        if command:
            if command.startswith('>>'):
                if current_block:
                    block_command = '\n'.join(current_block)
                    parsed_command = parse_command(block_command)

                    # Automatically load the database after creating it
                    if parsed_command:
                        if database is None and parsed_command['action'] not in ['create_database', 'load_database']:
                            print("\033[91mError: Database must be loaded first.\033[0m")
                            return

                        # Execute the command and update the database reference
                        database = execute_command(parsed_command, database)

                current_block = [command[2:].strip()]
            elif command.startswith('<<'):
                pass  # Ignore comment lines
            else:
                current_block.append(command)

    if current_block:
        block_command = '\n'.join(current_block)
        parsed_command = parse_command(block_command)

        # Automatically load the database after creating it
        if parsed_command:
            if database is None and parsed_command['action'] not in ['create_database', 'load_database']:
                print("\033[91mError: Database must be loaded first.\033[0m")
                return

            # Execute the command and update the database reference
            database = execute_command(parsed_command, database)

    # Save the database after running commands if it's loaded
    if database:
        database.save()




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("\033[91mUsage: python cldm_runner.py <file.cldm>\033[0m")  # Red for error messages
        sys.exit(1)
    
    cldm_file_path = sys.argv[1]

    if not cldm_file_path.endswith('.cldm'):
        print("\033[91mError: The command file must have a .cldm extension.\033[0m")  # Red for error messages
        sys.exit(1)
    
    start_time = time.time()
    
    # Call the function to process CLDM file commands
    execute_file_commands(cldm_file_path)
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.4f} seconds")
    
    input("Press Enter to exit...")