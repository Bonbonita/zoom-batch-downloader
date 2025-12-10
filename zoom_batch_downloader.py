import datetime
import math
import os
import sys
import traceback
import time
import shutil
import subprocess
import json
import re
from calendar import monthrange
from types import ModuleType

import colorama
from colorama import Fore, Style

import utils
from zoom_client import zoom_client

colorama.init()

try:
    import config as CONFIG
except ImportError:
    utils.print_bright_red('Missing config file, copy config_template.py to config.py and change as needed.')

client = zoom_client(
    account_id=CONFIG.ACCOUNT_ID, client_id=CONFIG.CLIENT_ID, client_secret=CONFIG.CLIENT_SECRET
)

# Global list to track skipped meetings
SKIPPED_MEETINGS = []

def delete_files_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def main():
    CONFIG.OUTPUT_PATH = utils.prepend_path_on_windows(CONFIG.OUTPUT_PATH)

    # Delete files in the output folder if DELETE_FILES_BEFORE_DOWNLOAD is True
    if CONFIG.DELETE_FILES_BEFORE_DOWNLOAD:
        delete_files_in_folder(CONFIG.OUTPUT_PATH)

    print_filter_warnings()

    if CONFIG.USE_EXACT_DATE:
        # Prompt the user to enter the date
        year = int(input("Enter the year: "))
        month = int(input("Enter the month: "))
        day = int(input("Enter the day: "))
        from_date = datetime.datetime(year, month, day)
        to_date = from_date
    else:
        from_date = datetime.datetime(CONFIG.START_YEAR, CONFIG.START_MONTH, CONFIG.START_DAY or 1)
        to_date = datetime.datetime(
            CONFIG.END_YEAR, CONFIG.END_MONTH, CONFIG.END_DAY or monthrange(CONFIG.END_YEAR, CONFIG.END_MONTH)[1],
        )

    # Check if the from_date is Friday
    if CONFIG.CHECK_FRIDAY_WEEKENDS and from_date.weekday() == 4:  # 0 is Monday, so 4 is Friday
        to_date += datetime.timedelta(days=2)  # Add two days if from_date is Friday

    file_count, total_size, skipped_count = download_recordings(get_users(), from_date, to_date)

    total_size_str = utils.size_to_string(total_size)

    print(
        f'{Style.BRIGHT}Downloaded {Fore.GREEN}{file_count}{Fore.RESET} files.',
        f'Total size: {Fore.GREEN}{total_size_str}{Fore.RESET}.{Style.RESET_ALL}',
        f'Skipped: {skipped_count} files.'
    )

    # Print warning summary at the end
    if SKIPPED_MEETINGS:
        print()
        utils.print_bright_red("!!! WARNING: Some meetings were skipped during scanning !!!")
        for item in SKIPPED_MEETINGS:
            print(f"- {item}")
        print()


def print_filter_warnings():
    did_print = False

    if CONFIG.TOPICS:
        utils.print_bright(f'Topics filter is active {CONFIG.TOPICS}')
        did_print = True
    if CONFIG.USERS:
        utils.print_bright(f'Users filter is active {CONFIG.USERS}')
        did_print = True
    if CONFIG.RECORDING_FILE_TYPES:
        utils.print_bright(f'Recording file types filter is active {CONFIG.RECORDING_FILE_TYPES}')
        did_print = True
        
    if did_print:
        print()

def get_users():
    if CONFIG.USERS:
        return [(email, '') for email in CONFIG.USERS]

    utils.print_bright('Scanning for users:')
    active_users_url = 'https://api.zoom.us/v2/users?status=active'
    inactive_users_url = 'https://api.zoom.us/v2/users?status=inactive'
    
    users = []
    pages = utils.chain(client.paginate(active_users_url), client.paginate(inactive_users_url))
    for page in utils.percentage_tqdm(pages):
        if CONFIG.CHECK_ONLY_LICENSED:
            users.extend([(user['email'], get_user_name(user)) for user in page['users'] if user['type'] == 2])
        else:
            users.extend([(user['email'], get_user_name(user)) for user in page['users']])

    print()
    return users

def get_user_name(user_data):
    first_name = user_data.get("first_name")
    last_name = user_data.get("last_name")

    if first_name and last_name:
        return f'{first_name} {last_name}'
    else:
        return first_name or last_name
    
def download_recordings(users, from_date, to_date):
    file_count, total_size, skipped_count = 0, 0, 0

    for user_email, user_name in users:
        user_description = get_user_description(user_email, user_name)
        user_host_folder = get_user_host_folder(user_email)

        utils.print_bright(
            f'Downloading recordings from user {user_description} - Starting at {date_to_str(from_date)} '
            f'and up to {date_to_str(to_date)} (inclusive).'
        )
        
        # This call now returns tuples of (uuid, topic)
        meeting_data = get_meeting_uuids(user_email, from_date, to_date)
        
        meetings = get_meetings(meeting_data)
        user_file_count, user_total_size, user_skipped_count = download_recordings_from_meetings(meetings, user_host_folder)

        utils.print_bright('######################################################################')
        print()
        
        file_count += user_file_count
        total_size += user_total_size
        skipped_count += user_skipped_count
    
    return (file_count, total_size, skipped_count)

def get_user_description(user_email, user_name):
    return f'{user_email} ({user_name})' if (user_name) else user_email

def get_user_host_folder(user_email):
    if CONFIG.GROUP_BY_USER:
        return os.path.join(CONFIG.OUTPUT_PATH, user_email)
    else:
        return CONFIG.OUTPUT_PATH
    
def date_to_str(date):
    return date.strftime('%Y-%m-%d')

def get_meeting_uuids(user_email, start_date, end_date):
    meeting_data_list = []

    local_start_date = start_date
    delta = datetime.timedelta(days=29)
    
    utils.print_bright('Scanning for recorded meetings:')
    estimated_iterations = math.ceil((end_date-start_date) / datetime.timedelta(days=30))
    with utils.percentage_tqdm(total=estimated_iterations) as progress_bar:
        while local_start_date <= end_date:
            local_end_date = min(local_start_date + delta, end_date)

            local_start_date_str = date_to_str(local_start_date)
            local_end_date_str = date_to_str(local_end_date)
            url = f'https://api.zoom.us/v2/users/{user_email}/recordings?from={local_start_date_str}&to={local_end_date_str}'
            
            data_chunk = []
            for page in client.paginate(url):
                # Now capturing both UUID and Topic
                data_chunk.extend([(meeting['uuid'], meeting.get('topic', 'Unknown Topic')) for meeting in page['meetings']])

            meeting_data_list.extend(reversed(data_chunk))
            local_start_date = local_end_date + datetime.timedelta(days=1)
            progress_bar.update(1)

    return meeting_data_list

def get_meetings(meeting_data):
    meetings = []

    if meeting_data:
        utils.print_bright(f'Scanning for recordings:')
        # Loop through the tuples (uuid, topic)
        for meeting_uuid, meeting_topic in utils.percentage_tqdm(meeting_data):
            url = f'https://api.zoom.us/v2/meetings/{utils.double_encode(meeting_uuid)}/recordings'
            
            try:
                meetings.append(client.get(url))
            except Exception as e:
                # Catch the specific 3301 "processing" error
                if "3301" in str(e):
                    SKIPPED_MEETINGS.append(f"Meeting: '{meeting_topic}' (Still Processing)")
                    continue
                elif "404" in str(e):
                    SKIPPED_MEETINGS.append(f"Meeting: '{meeting_topic}' (Not Found / 404)")
                    continue
                else:
                    raise e

    return meetings

def download_recordings_from_meetings(meetings, host_folder):
    file_count, total_size, skipped_count = 0, 0, 0

    for meeting in meetings:
        if CONFIG.TOPICS and meeting['topic'] not in CONFIG.TOPICS and utils.slugify(meeting['topic']) not in CONFIG.TOPICS:
            continue
        
        recording_files = meeting.get('recording_files') or []
        participant_audio_files = (meeting.get('participant_audio_files') or []) if CONFIG.INCLUDE_PARTICIPANT_AUDIO else []

        for recording_file in recording_files + participant_audio_files:
            if 'file_size' not in recording_file:
                continue

            if CONFIG.RECORDING_FILE_TYPES and recording_file['file_type'] not in CONFIG.RECORDING_FILE_TYPES:
                continue

            url = recording_file['download_url']
            ext = (recording_file.get('file_extension') or os.path.splitext(recording_file['file_name'])[1]).lower()
            
            if CONFIG.USE_MEETING_TOPIC_NAME:
                topic = meeting['topic']
                file_name = f'{topic}.{ext}'
            else:
                topic = utils.slugify(meeting['topic'])
                recording_name = utils.slugify(f'{topic}__{recording_file["recording_start"]}')
                file_id = recording_file['id']
                file_name_suffix = os.path.splitext(recording_file['file_name'])[0] + '__' if 'file_name' in recording_file else ''
                recording_type_suffix = recording_file['recording_type'] + '__' if 'recording_type' in recording_file else ''
                file_name = utils.slugify(
                    f'{recording_name}__{recording_type_suffix}{file_name_suffix}{file_id[-8:]}'
                ) + '.' + ext
            
            file_size = int(recording_file['file_size'])

            if download_recording_file(url, host_folder, file_name, file_size, topic):
                total_size += file_size
                file_count += 1
            else:
                skipped_count += 1
    
    return file_count, total_size, skipped_count


def download_recording_file(download_url, host_folder, file_name, file_size, topic):
    # Replace / and \ characters in the file name
    file_name = re.sub(r'[\\/*?:"<>|]',"", file_name)

    # Check if the file size is less than the minimum size
    if file_size < CONFIG.MIN_FILE_SIZE * 1024 * 1024:  # Convert MIN_FILE_SIZE from MB to bytes
        print(f'Skipping: {file_name} (size is less than {CONFIG.MIN_FILE_SIZE} MB)')
        return False

    if CONFIG.VERBOSE_OUTPUT:
        print()
        utils.print_dim(f'URL: {download_url}')

    file_path = create_path(host_folder, file_name, topic, file_name)

    # Check if a file with the same name already exists
    if os.path.exists(file_path):
        base_name, ext = os.path.splitext(file_name)
        i = 1
        # Find a new file name that does not exist yet
        while os.path.exists(os.path.join(host_folder, f'{base_name}_{i}{ext}')):
            i += 1
        file_name = f'{base_name}_{i}{ext}'
        file_path = create_path(host_folder, file_name, topic, file_name)

    if os.path.exists(file_path) and abs(os.path.getsize(file_path) - file_size) <= CONFIG.FILE_SIZE_MISMATCH_TOLERANCE:
        utils.print_dim(f'Skipping existing file: {file_name}')
        return False
    elif os.path.exists(file_path):
        utils.print_dim_red(f'Deleting corrupt file: {file_name}')
        os.remove(file_path)

    utils.print_bright(f'Downloading: {file_name}')
    utils.wait_for_disk_space(file_size, CONFIG.OUTPUT_PATH, CONFIG.MINIMUM_FREE_DISK, interval=5)

    tmp_file_path = file_path + '.tmp'
    client.do_with_token(
        lambda t: utils.download_with_progress(
            f'{download_url}?access_token={t}', tmp_file_path, file_size, CONFIG.VERBOSE_OUTPUT,
            CONFIG.FILE_SIZE_MISMATCH_TOLERANCE
        )
    )
    
    os.rename(tmp_file_path, file_path)

    return True

def create_path(host_folder, file_name, topic, recording_name):
    folder_path = host_folder

    if CONFIG.GROUP_BY_TOPIC:
        folder_path = os.path.join(folder_path, topic)
    if CONFIG.GROUP_BY_RECORDING:
        folder_path = os.path.join(folder_path, recording_name)

    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, file_name)

def process_videos():
    # Initialize total time spent
    total_time_spent = 0

    # Use OUTPUT_PATH from config as the input and output folder
    input_output_folder = CONFIG.OUTPUT_PATH

    print(f"Processing videos in {input_output_folder}...")
    for filename in os.listdir(input_output_folder):
        if filename.lower().endswith((".mp4", ".avi", ".mkv", ".flv", ".mov")):  # Add or remove video formats as needed
            print(f"Processing {filename}...")
            input_file = os.path.join(input_output_folder, filename)
            output_file = os.path.join(input_output_folder, os.path.splitext(filename)[0] + "-proj.llc")
            log_file = os.path.join(input_output_folder, os.path.splitext(filename)[0] + ".txt")

            # Use noise and d values from CONFIG
            noise = CONFIG.NOISE
            d = CONFIG.DURATION

            command = f'ffmpeg -hide_banner -vn -i "{input_file}" -af silencedetect=noise={noise}dB:d={d} -f null - 2>&1'
            
            # Start the timer
            start_time = time.time()
            output = subprocess.check_output(command, shell=True).decode("utf-8")
            # End the timer
            end_time = time.time()
            
            # Calculate the time spent
            time_spent = end_time - start_time
            total_time_spent += time_spent  # Add the time spent on this video to the total
            print(f"Time spent on silent audio detection: {time_spent} seconds")

            with open(log_file, "w", encoding="utf-8") as f:
                f.write(output)

            cut_segments = []
            start = None
            end = None
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    if "[silencedetect @" in line:
                        if "silence_start" in line:
                            start = float(line.split(":")[1].strip())
                        elif "silence_end" in line:
                            end = float(line.split(":")[1].split("|")[0].strip())
                            if start is not None and end is not None and end > start:
                                cut_segments.append({"start": start, "end": end, "name": ""})
                                start = None
                                end = None

            llc_data = {
                "version": 1,
                "mediaFileName": filename,
                "cutSegments": cut_segments
            }

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(llc_data, f, indent=2, ensure_ascii=False)

            # Delete the temporary log file
            os.remove(log_file)
            print(f"Finished processing {filename}.")
    print(f"Finished processing videos in {input_output_folder}.")

    # Print the total time spent on processing videos
    print(f"Total time spent on processing videos: {total_time_spent} seconds")



if __name__ == '__main__':
    try:
        import config as CONFIG
    except ImportError:
        utils.print_bright_red('Missing config file, copy config_template.py to config.py and change as needed.')

    try:
        main()

        # Call the process_videos function if GENERATE_LLC_FILES is True
        if CONFIG.GENERATE_LLC_FILES:
            process_videos()

    except AttributeError as error:
        if error.obj.__name__ == 'config':
            print()
            utils.print_bright_red(
                f'Variable {error.name} is not defined in config.py. '
                f'See config_template.py for the complete list of variables.'
            )
        else:
            raise

    except Exception as error:
        print()
        if not getattr(CONFIG, "VERBOSE_OUTPUT"):
            utils.print_bright_red(f'Error: {error}')
        elif utils.is_debug():
            raise
        else:
            utils.print_dim_red(traceback.format_exc())

    except KeyboardInterrupt:
        print()
        utils.print_bright_red('Interrupted by the user')
        exit(1)
