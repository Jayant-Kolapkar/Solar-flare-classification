import os
from datetime import datetime, timedelta
from pathlib import Path

# Priority for flare dominance (higher value = more dominant)
flare_priority = {'X': 5, 'M': 4, 'C': 3, 'B': 2, 'A': 1}

# Class-specific prediction windows: [before_hours, after_hours]
prediction_windows = {
    'X': [24, 24],
    'M': [12, 12],
    'C': [6, 6],
    'B': [12, 12],
    'A': [24, 24]
}

def parse_ngdc_line(line):
    """Parse a line from the NGDC flare catalog."""
    try:
        line = line.strip()
        if len(line) < 25:
            return None

        date_part = line[:11]  # e.g., 120101
        day_str = date_part[-2:]
        month_str = date_part[-4:-2]
        year_str = date_part[-6:-4]

        times = line[11:27].split()
        if len(times) < 3:
            return None

        start_time, _, _ = times[:3]

        year = 2000 + int(year_str)
        month = int(month_str)
        day = int(day_str)
        hour = int(start_time[:2])
        minute = int(start_time[2:])
        event_time = datetime(year, month, day, hour, minute)

        for i in range(len(line)-1, -1, -1):
            if line[i] in flare_priority:
                return {'datetime': event_time, 'class': line[i]}
        return None
    except:
        return None

def get_flare_events(ngdc_file_path):
    """Load and parse flare events from NGDC text file."""
    flare_events = []
    with open(ngdc_file_path, 'r') as file:
        for line in file:
            parsed = parse_ngdc_line(line)
            if parsed:
                flare_events.append(parsed)
    return flare_events

def assign_label(timestamp, flare_events):
    """
    Given an image timestamp, assign it the most dominant flare class
    based on class-specific predictive windows.
    """
    dominant_class = None
    highest_priority = 0

    for event in flare_events:
        flare_time = event['datetime']
        flare_class = event['class']
        if flare_class not in prediction_windows:
            continue

        window_before, window_after = prediction_windows[flare_class]
        start_time = flare_time - timedelta(hours=window_before)
        end_time = flare_time + timedelta(hours=window_after)

        if start_time <= timestamp <= end_time:
            if flare_priority[flare_class] > highest_priority:
                dominant_class = flare_class
                highest_priority = flare_priority[flare_class]

    return dominant_class  # Can be None (no flare found)

def organize_images(ngdc_file_path, image_dir, output_dir):
    """Classify images into folders based on flare association."""
    ngdc_file_path = Path(ngdc_file_path)
    image_dir = Path(image_dir)
    output_dir = Path(output_dir)

    flare_events = get_flare_events(ngdc_file_path)

    detailed_dir = output_dir / 'detailed_classification'
    for flare_class in flare_priority.keys():
        (detailed_dir / f'next24h_{flare_class}').mkdir(parents=True, exist_ok=True)
    (detailed_dir / 'no_flare').mkdir(parents=True, exist_ok=True)

    for filename in os.listdir(image_dir):
        if filename.endswith('.jp2'):
            try:
                img_time = datetime.strptime(filename[:-4], '%Y%m%d_%H%M')
                assigned_class = assign_label(img_time, flare_events)

                src_path = image_dir / filename
                if assigned_class:
                    dst_path = detailed_dir / f'next24h_{assigned_class}' / filename
                else:
                    dst_path = detailed_dir / 'no_flare' / filename

                os.link(src_path, dst_path)
            except Exception as e:
                print(f"Skipping {filename}: {e}")

if __name__ == "__main__":

    dataset_path = Path("E:/BTP/dataset_12_to_18")

    ngdc_file = dataset_path.parent / "ngdc" / "paste.txt"
    image_dir = dataset_path
    output_dir = dataset_path.parent / "classified_dataset"

    print(f"NGDC File Path: {ngdc_file}")
    print(f"Image Directory Path: {image_dir}")
    print(f"Output Directory Path: {output_dir}")

    organize_images(ngdc_file, image_dir, output_dir)
