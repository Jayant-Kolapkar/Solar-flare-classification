import os
from datetime import datetime, timedelta
from pathlib import Path

def parse_ngdc_line(line):
    """Parse a line from the NGDC data file with corrected date parsing."""
    try:
        # Remove any leading/trailing whitespace
        line = line.strip()
        
        # Debug print
        print(f"Parsing line: {line}")
        
        if len(line) < 25:  # Minimum length check
            print(f"Line too short: {len(line)} characters")
            return None
            
        # Extract and print each component for debugging
        date_part = line[:11]  # 31777041231
        print(f"Date part: {date_part}")
        
        # Parse YYMMDD from the end of the date_part
        # For 31777041231, we want 12 (MM) 31 (DD) 04 (YY)
        day_str = date_part[-2:]    # Last 2 digits
        month_str = date_part[-4:-2]  # Next 2 digits from end
        year_str = date_part[-6:-4]   # Next 2 digits from end
        
        print(f"Year: {year_str}, Month: {month_str}, Day: {day_str}")
        
        # Parse times
        times = line[11:27].split()  # Should give us three times
        print(f"Times extracted: {times}")
        
        if len(times) >= 3:
            start_time, peak_time, end_time = times[:3]
            print(f"Start: {start_time}, Peak: {peak_time}, End: {end_time}")
            
            # Convert to integers for validation
            year = 2000 + int(year_str)
            month = int(month_str)
            day = int(day_str)
            hour = int(peak_time[:2])
            minute = int(peak_time[2:])
            
            # Validate components
            if not (1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
                print("Invalid date/time components")
                return None
                
            # Create datetime object
            date_obj = datetime(year, month, day, hour, minute)
            print(f"Datetime created: {date_obj}")
            
            # Look for flare class
            for i in range(len(line)-1, -1, -1):  # Search from end
                if line[i] in ['X', 'M', 'C', 'B', 'A']:
                    flare_class = line[i]
                    print(f"Found flare class: {flare_class}")
                    return {
                        'datetime': date_obj,
                        'class': flare_class
                    }
            
            print("No flare class found")
            return None
            
        else:
            print("Couldn't parse times correctly")
            return None
            
    except Exception as e:
        print(f"Error parsing line: {str(e)}")
        return None

def get_flare_events(ngdc_file_path):
    """Read and parse all flare events from the NGDC file."""
    flare_events = []
    print(f"\nReading file: {ngdc_file_path}")
    
    try:
        with open(ngdc_file_path, 'r') as file:
            print("File opened successfully")
            for line_num, line in enumerate(file, 1):
                print(f"\nProcessing line {line_num}:")
                parsed = parse_ngdc_line(line)
                if parsed:
                    flare_events.append(parsed)
                    print("Successfully parsed and added event")
                else:
                    print("Failed to parse line")
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        return []

    print(f"\nTotal events parsed: {len(flare_events)}")
    if flare_events:
        print(f"First event: {flare_events[0]['datetime']} Class {flare_events[0]['class']}")
        print(f"Last event: {flare_events[-1]['datetime']} Class {flare_events[-1]['class']}")
    
    return flare_events

def test_parser():
    """Test the parser with a sample line."""
    print("Testing parser with sample line...")
    # Test with your actual line
    sample_line = "31777041231  2222 2231 2226                                B 20    GOES 1.0E-04"
    result = parse_ngdc_line(sample_line)
    if result:
        print(f"Successfully parsed test line: {result}")
    else:
        print("Failed to parse test line")

if __name__ == "__main__":
    # First run the test parser
    test_parser()
    
    # Then try to parse the actual file
    base_dir = Path.cwd()
    ngdc_file = base_dir / "data" / "ngdc" / "paste.txt"
    
    print(f"\nAttempting to parse file: {ngdc_file.absolute()}")
    flare_events = get_flare_events(ngdc_file)