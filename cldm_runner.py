import re
import sys

# Define regex patterns for each command type
patterns = {
    'create_loop': r'^FORGE LOOP (\w+)$',
    'segment_loop': r'^SEGMENT LOOP (\w+) INTO (\w+)$',
    'craft': r'^CRAFT "([^"]+)" INTO (\w+) IN (\w+)$',
    'change_master': r'^CHANGE MASTER SEGMENT OF (\w+) TO (\w+)$'
}

def parse_command(command):
    print(f"\033[93mParsing command: {command}\033[0m")  # Yellow for debug statements
    for action, pattern in patterns.items():
        print(f"\033[93mTrying pattern for action '{action}': {pattern}\033[0m")  # Yellow for debug statements
        match = re.match(pattern, command, re.IGNORECASE)
        if match:
            print(f"\033[93mPattern matched for action '{action}'\033[0m")  # Yellow for debug statements
            if action == 'create_loop':
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
    print("\033[93mNo matching pattern found.\033[0m")  # Yellow for debug statements
    return None

def execute_command(parsed_command, database):
    print(f"\033[93mExecuting command: {parsed_command}\033[0m")  # Yellow for debug statements
    if parsed_command['action'] == "create_loop":
        loop_name = parsed_command['loop_name']
        database[loop_name] = {}
        print(f"\033[92mLoop {loop_name} created.\033[0m")  # Green for user-visible text
    
    elif parsed_command['action'] == "segment_loop":
        loop_name = parsed_command['loop_name']
        segment_name = parsed_command['segment_name']
        if loop_name in database:
            database[loop_name][segment_name] = []
            print(f"\033[92mSegment {segment_name} added to Loop {loop_name}.\033[0m")  # Green for user-visible text
        else:
            print(f"\033[91mLoop {loop_name} does not exist.\033[0m")  # Red for error messages
    
    elif parsed_command['action'] == "craft":
        loop_name = parsed_command['loop_name']
        segment_name = parsed_command['segment_name']
        value = parsed_command['value']
        print(f"\033[93mCrafting value {value} in segment {segment_name} in Loop {loop_name}.\033[0m")  # Yellow for debug
        if loop_name in database and segment_name in database[loop_name]:
            database[loop_name][segment_name].append(value)
            print(f"\033[92mValue {value} added to Segment {segment_name} in Loop {loop_name}.\033[0m")  # Green for user-visible text
            
            # Check for master segment
            check_master_segment(database[loop_name])
        else:
            print(f"\033[91mLoop or Segment does not exist.\033[0m")  # Red for error messages
    
    elif parsed_command['action'] == "select":
        loop_name = parsed_command['loop_name']
        condition_key, condition_value = parsed_command['condition'].split('=')
        condition_key = condition_key.strip()
        condition_value = condition_value.strip().strip('"')
        print(f"\033[93mSelecting from loop {loop_name} where {condition_key} = {condition_value}\033[0m")  # Yellow for debug
        
        if loop_name in database:
            result = []
            for segment_name, values in database[loop_name].items():
                if segment_name == condition_key:
                    for value in values:
                        if value == condition_value:
                            result.append({attr: value for attr in parsed_command['attributes']})
            if result:
                print(f"\033[92mResult: {result}\033[0m")  # Green for user-visible text
            else:
                print(f"\033[91mNo matching records found.\033[0m")  # Red for error messages
        else:
            print(f"\033[91mLoop {loop_name} does not exist.\033[0m")  # Red for error messages
    
    elif parsed_command['action'] == "change_master":
        loop_name = parsed_command['loop_name']
        segment_name = parsed_command['segment_name']
        print(f"\033[93mAttempting to change master segment to {segment_name} for loop {loop_name}\033[0m")  # Yellow for debug

        if loop_name in database:
            segment_values = database[loop_name].get(segment_name, [])
            if len(segment_values) == len(set(segment_values)):
                database[loop_name]['MasterSegment'] = segment_name
                print(f"\033[92mMaster Segment changed to {segment_name} for Loop {loop_name}.\033[0m")  # Green for user-visible text
            else:
                print(f"\033[91mError: Segment {segment_name} contains non-unique values and cannot be used as the Master Segment.\033[0m")  # Red for error messages
        else:
            print(f"\033[91mLoop {loop_name} does not exist.\033[0m")  # Red for error messages
    
    else:
        print("\033[91mInvalid command.\033[0m")  # Red for error messages

def check_master_segment(loop_data):
    # Check if any segment has unique values
    current_master = loop_data.get('MasterSegment', None)
    unique_segment = None

    for segment_name, values in loop_data.items():
        if len(values) == len(set(values)):
            unique_segment = segment_name
            break

    if unique_segment and unique_segment != current_master:
        loop_data['MasterSegment'] = unique_segment
        print(f"\033[92mMaster Segment selected: {unique_segment}\033[0m")  # Green for user-visible text
    elif not unique_segment and not current_master:
        # Create a new Master Segment with unique values
        loop_data['MasterSegment'] = ['1', '2', '3', '4', '5'][:len(loop_data[next(iter(loop_data))])]
        print(f"\033[92mNew Master Segment created with unique values: {loop_data['MasterSegment']}\033[0m")  # Green for user-visible text

def execute_file_commands(file_path):
    database = {}

    with open(file_path, 'r') as file:
        commands = file.readlines()

    current_block = []

    for command in commands:
        command = command.strip()
        if command:
            if command.startswith('>>'):
                if current_block:
                    block_command = '\n'.join(current_block)
                    parsed_command = parse_command(block_command)
                    if parsed_command:
                        execute_command(parsed_command, database)
                current_block = [command[2:].strip()]
            elif command.startswith('<<'):
                pass
            else:
                current_block.append(command)

    if current_block:
        block_command = '\n'.join(current_block)
        parsed_command = parse_command(block_command)
        if parsed_command:
            execute_command(parsed_command, database)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("\033[91mUsage: cldm.exe <file.cldm>\033[0m")  # Red for error messages
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not file_path.endswith('.cldm'):
        print("\033[91mError: The file must have a .cldm extension\033[0m")  # Red for error messages
        sys.exit(1)
    
    execute_file_commands(file_path)
    
input("Press Enter to exit...")
