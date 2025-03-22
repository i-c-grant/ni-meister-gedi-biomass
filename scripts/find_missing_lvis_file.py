import os

def get_lvis_url_keys(url_file, extension):

    keys = []

    if extension[0] != '.':
        extension = '.' + extension

    with open(url_file, 'r') as f:
        for line in f:
            url = line.strip()

            # Extract the filename from the URL
            filename = os.path.basename(url)

            # Remove extension
            stem, ext = os.path.splitext(filename)

            # Split stem by underscores
            if ext == extension:
                parts = stem.split('_')
                if len(parts) == 5:
                    result = '_'.join(parts[2:5])
                    keys.append(result)
                else:
                    print(f"Unexpected filename format: {filename}")

    return keys
            
def get_lvis_file_keys(directory, extension):
    keys = []

    if extension[0] != '.':
        extension = '.' + extension

    for filename in os.listdir(directory):
        if filename.endswith(extension):
            # Remove extension
            stem, ext = os.path.splitext(filename)

            # Split stem by underscores
            parts = stem.split('_')
            if len(parts) == 5:
                result = '_'.join(parts[2:5])
                keys.append(result)
            else:
                print(f"Unexpected filename format: {filename}")

    return keys
