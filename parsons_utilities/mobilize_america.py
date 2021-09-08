from requests import request as _request
from parsons_utilities.table import Table
from parsons_utilities.datetime import date_to_timestamp
import petl
import re
import os
import logging

logger = logging.getLogger(__name__)

MA_URI = 'http://events.mobilizeamerica.io/api/v1/'

class MobilizeAmerica(object):
    """
    Instantiate MobilizeAmerica Class

    api_key: str
        An api key issued by Mobilize America. This is required to access some private methods.

    `Returns:`
        MobilizeAmerica Class
    """

    def __init__(self, api_key=None):

        self.uri = MA_URI
        self.api_key = api_key or os.environ.get('MOBILIZE_AMERICA_API_KEY')

        if not self.api_key:
            logger.info('Mobilize America API Key missing. Calling methods that rely on private'
                        ' endpoints will fail.')

    def _request(self, url, req_type='GET', post_data=None, args=None, auth=False):
        if auth:

            if not self.api_key:
                raise TypeError('This method requires an api key.')
            else:
                header = {'Authorization': 'Bearer ' + self.api_key}

        else:
            header = None

        r = _request(req_type, url, json=post_data, params=args, headers=header)

        if 'error' in r.json():
            raise ValueError('API Error:' + str(r.json()['error']))

        json = r.json()['data']

        print(f"URL: {url}")
        print(f"NEXT: {r.json()['next']}")

        while r.json()['next']:
            url = r.json()['next']
            r = _request(req_type, url, json=post_data, params=args, headers=header)
            json.extend(r)

        return json

    def organizations(self, updated_since=None):
        """
        Return all active organizations on the platform.

        `Args:`
            updated_since: str
                Filter to organizations updated since given date (ISO Date)
        `Returns`
            JSON of paginated response data.

        `Sample usage`

        mob = MobilizeAmerica()
        organizations_json = mob.get_organizations_json(updated_since='2021-09-01')
        """

        json_response = self._request(self.uri + 'organizations',
                             args={
                                 'updated_since': date_to_timestamp(updated_since)
                             })

        output_dict = dict()
        for i in range(0, len(json_response)):
            row_id = str(i)
            output_dict[row_id] = json_response[i]

        return output_dict

    def get_events(self, organization_id=None, updated_since=None, timeslot_start=None,
                   timeslot_end=None, timeslots_table=False, max_timeslots=None, output_format='Parsons'):
        """
        Fetch all public events on the platform.

        `Args:`
            organization_id: list or int
                Filter events by a single or multiple organization ids
            updated_since: str
                Filter to events updated since given date (ISO Date)
            timeslot_start: str
                Filter by a timeslot start of events using ``>``,``>=``,``<``,``<=``
                operators and ISO date (ex. ``<=2018-12-13 05:00:00PM``)
            timeslot_end: str
                Filter by a timeslot end of events using ``>``,``>=``,``<``,``<=``
                operators and ISO date (ex. ``<=2018-12-13 05:00:00PM``)
            timeslot_table: boolean
                Return timeslots as a separate long table. Useful for extracting
                to databases.
            max_timeslots: int
                If not returning a timeslot table, will unpack time slots. If do not
                set this kwarg, it will add a column for each time slot. The argument
                limits the number of columns and discards any additional timeslots
                after that.

                For example: If there are 20 timeslots associated with your event,
                and you set the max time slots to 5, it will only return the first 5
                time slots as ``time_slot_0``, ``time_slot_1`` etc.

                This is helpful in situations where you have a regular sync
                running and want to ensure that the column headers remain static.

        `Returns`
            either Parsons Table or dict or Parsons Tables (See :ref:`parsons-table` for output options)
            OR a JSON object (e.g. for use in Airbyte integration)
        """

        if isinstance(organization_id, (str, int)):
            organization_id = [organization_id]

        args = {'organization_id': organization_id,
                'updated_since': date_to_timestamp(updated_since),
                'timeslot_start': self._time_parse(timeslot_start),
                'timeslot_end': self._time_parse(timeslot_end)}

        json_response = self._request(self.uri + 'events', args=args, suppress_args_on_paginate=True)

        tbl = Table(json_response)

        if tbl.num_rows > 0:

            tbl.unpack_dict('sponsor')
            tbl.unpack_dict('location', prepend=False)
            tbl.unpack_dict('location', prepend=False)  # Intentional duplicate
            tbl.table = petl.convert(tbl.table, 'address_lines', lambda v: ' '.join(v))

            if timeslots_table:

                timeslots_tbl = tbl.long_table(['id'], 'timeslots', {'id': 'event_id'})
                return {'events': tbl, 'timeslots': timeslots_tbl}

            else:
                tbl.unpack_list('timeslots', replace=True, max_columns=max_timeslots)
                cols = tbl.columns
                for c in cols:
                    if re.search('timeslots', c, re.IGNORECASE) is not None:
                        tbl.unpack_dict(c)

        if output_format == 'Parsons':

            return tbl

        elif output_format == 'JSON':
            # For the Airbyte integration, we need to output a JSON object
            # To get from the Parsons table object to a JSON, we can save the table as a csv, then use petl to extract the csv and load it into a json, which we then return

            # Write table to a CSV
            csv_filename = tbl.to_csv()

            # Extract the CSV into a new petl table
            testcsv = fromcsv(csv_filename)

            # Load the petl table into a json file
            tojson(testcsv, 'output.json')  # convert the CSV to json

            # Load the json file into a JSON object
            f = open('output.json')
            json_output = json.load(f)

            return json_output
