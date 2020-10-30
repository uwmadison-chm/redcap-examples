#!/usr/bin/env python

"""
Downloads the data for a longitudinal REDCap project, with each event as 
a separate file. Also allows filtering by standard REDCap logic, so you can,
for example, exclude non-enrolled participants from your data.

Requires an instrument_download_list_file, which is a CSV file containing
the fields "instrument_name" and "download". If download is not blank, the
instrument will be included in the download.

Only instruments belonging to the given event are downloaded in the event's
file, to keep file sizes down and column counts more manageable.

The API URL, API token, and filter logic are all passed using environment
variables: API_URL, API_TOK, and FILTER, respectively.

Requires the PyCap library: https://pycap.readthedocs.io/en/latest/
"""

import sys
import os

import redcap
import pandas as pd

from pathlib import Path

import logging
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


API_URL = os.environ['REDCAP_API_URL']
API_TOK = os.environ['REDCAP_API_TOKEN']
FILTER = os.environ.get('REDCAP_FILTER_LOGIC')
PROJ = redcap.Project(API_URL, API_TOK)


def filtered_ids():
    id_field = PROJ.field_names[0]
    first_event = PROJ.events[0]
    record_names = PROJ.export_records(
        fields=[id_field],
        filter_logic=FILTER,
        format='df'
    )
    logger.debug(record_names.index)
    return list(record_names.index.get_level_values(0))


def main(instrument_download_list_file, out_path):
    # We need this because we can't filter using data that doesn't occur
    # in the target event, because redcap is kinda dumb
    ids = filtered_ids()
    form_events = PROJ.export_fem(format='df')
    all_form_list = pd.read_csv(instrument_download_list_file)
    selected_forms = frozenset(all_form_list.dropna()['instrument_name'])
    logger.debug(f'Forms to download: {selected_forms}')

    for event_name, event_rows in form_events.groupby(by='unique_event_name'):
        available_forms = frozenset(event_rows['form'])
        download_forms = selected_forms & available_forms
        logger.debug(f'Event {event_name}: Downloading {download_forms}')
        data = PROJ.export_records(
            records=ids,
            events=[event_name],
            forms=download_forms,
            export_survey_fields=False,
            export_checkbox_labels=True,
            format='df',
            df_kwargs={
                'dtype': 'str'
            }
        )
        out_filename = out_path / f'{event_name}.csv'
        data.to_csv(out_filename, index=False)


if __name__ == "__main__":
    instrument_download_list_file = sys.argv[1]
    out_dir = sys.argv[2]
    out_path = Path(out_dir)
    main(instrument_download_list_file, out_path)
