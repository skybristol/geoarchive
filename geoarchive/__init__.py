import hashlib
import ahocorasick
import numpy as np
import re
from dateutil import parser

def calculate_checksum(file_path: str, checksum_type: str = 'sha256'):
    with open(file_path, 'rb') as file:
        if checksum_type == 'md5':
            h = hashlib.md5()
        elif checksum_type == 'sha256':
            h = hashlib.sha256()
        chunk = file.read(8192)
        while chunk:
            h.update(chunk)
            chunk = file.read(8192)
    return h.hexdigest()

def extract_linkable_terms(documents: list, terms: dict):
    A = ahocorasick.Automaton()

    for idx, term in enumerate(terms.keys()):
        A.add_word(term, (idx, term))

    A.make_automaton()

    matches = {} 
    for doc in documents:
        for end_index, (idx, term) in A.iter(doc):
            start_index = end_index - len(term) + 1
            if term in matches:
                matches[term] += 1
            else:
                matches[term] = 1

    frequencies = np.array(list(matches.values()))
    mean_freq = np.mean(frequencies)
    std_freq = np.std(frequencies)

    z_scores = {term: (count - mean_freq) / std_freq for term, count in matches.items()}

    return {term: terms[term] for term, z in z_scores.items() if z > 2}

def extract_date(document: str):
    date_patterns = [
        r'\b\d{4}-\d{2}-\d{2}\b',  # YYYY-MM-DD
        r'\b\d{2}/\d{2}/\d{4}\b',  # MM/DD/YYYY
        r'\b\d{2}-\d{2}-\d{4}\b',  # DD-MM-YYYY
        r'\b\d{1,2} \w+ \d{4}\b',  # D Month YYYY or DD Month YYYY
        r'\b\w+ \d{1,2}, \d{4}\b',  # Month D, YYYY or Month DD, YYYY
        r'\b\d{1,2}(?:st|nd|rd|th)? \w+ \d{4}\b',  # D(th) Month YYYY or DD(th) Month YYYY
        r'\b\w+ \d{1,2}(?:st|nd|rd|th)?, \d{4}\b'  # Month D(th), YYYY or Month DD(th), YYYY
    ]

    found_dates = []
    for pattern in date_patterns:
        matches = re.findall(pattern, document)
        found_dates.extend(matches)

    if found_dates:
        found_dates = list(set(found_dates))

        valid_dates = []
        for date_str in found_dates:
            try:
                date_obj = parser.parse(date_str)
                valid_dates.append(date_obj)
            except ValueError:
                continue

        if valid_dates:
            return max(valid_dates).isoformat()
        else:
            return None
