from parsons_utilities.table import Table
from parsons_utilities.datetime import date_to_timestamp

import json
import logging
import os
import petl
from petl import fromcsv, tojson
import re
from requests import request as _request

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

    def _request(self, url, req_type='GET', post_data=None, args=None, auth=False, suppress_args_on_paginate=False): #suppress_args_on_paginate added 9/8/21 to correct for behavior on some endpoints where the "next" URL already contains the query params that "args" wants to pass in (causing the request to fail)
        if auth:

            if not self.api_key:
                raise TypeError('This method requires an api key.')
            else:
                header = {'Authorization': 'Bearer ' + self.api_key}

        else:
            header = None

        r = _request(req_type, url, json=post_data, params=args, headers=header)
        #print(f"Running: r = _request(method='{req_type}', url='{url}', json={post_data}, params={args},headers={header})")

        if 'error' in r.json():
            raise ValueError('API Error:' + str(r.json()['error']))

        json = r.json()['data']
        #print(f"Running: json = r.json()['data']")

        while r.json()['next']:
            url = r.json()['next']
            if suppress_args_on_paginate:
                args=None
            r = _request(req_type, url, json=post_data, params=args, headers=header)
            #print(f"Running: r = _request(method='{req_type}', url='{url}', json={post_data}, params={args},headers={header})")
            #print(f"Running: json.extend(r.json()['data'])")
            json.extend(r.json()['data'])

        return json

    def _time_parse(self, time_arg):
        # Parse the date filters

        trans = [('>=', 'gte_'),
                 ('>', 'gt_'),
                 ('<=', 'lte_'),
                 ('<', 'lt_')]

        if time_arg:

            time = re.sub('<=|<|>=|>', '', time_arg)
            time = date_to_timestamp(time)
            time_filter = re.search('<=|<|>=|>', time_arg).group()

            for i in trans:
                if time_filter == i[0]:
                    return i[1] + str(time)

            raise ValueError('Invalid time operator. Must be one of >=, >, <= or >.')

        return time_arg

    '''
    **************************************
    ****************ROUTES****************
    **************************************
    '''

    def get_organizations(self, updated_since=None, output_format='Parsons'):
        """
        Return all active organizations on the platform.

        `Args:`
            updated_since: str
                Filter to organizations updated since given date (ISO Date)
        `Returns`
            Parsons Table
                See :ref:`parsons-table` for output options.
        """

        valid_output_formats = ['Parsons', 'JSON']

        if output_format not in valid_output_formats:
            raise ValueError(f'Invalid output_format (must be one of: {self.output_formats}')

        json_response = self._request(self.uri + 'organizations',
                                   args={
                                       'updated_since': date_to_timestamp(updated_since)
                                   })

        if output_format=='Parsons':

            return Table(json_response)

        elif output_format=='JSON':

            return json_response

    def get_events_auth(self, organization_id=None, updated_since=None, timeslot_start=None,
                                timeslot_end=None, timeslots_table=False, unpack_timeslots=True, max_timeslots=None, zipcode=None, max_dist=None, visibility=None,
                                exclude_full=False, is_virtual=None, event_types=None, output_format='Parsons'):
        """
        Fetch all public events for an organization. This includes both events owned
        by the organization (as indicated by the organization field on the event object)
        and events of other organizations promoted by this specified organization.

        .. note::
            API Key Required

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
            zipcode: str
                Filter by a Events' Locations' postal code. If present, returns Events
                sorted by distance from zipcode. If present, virtual events will not be returned.
            max_dist: str
                Filter Events' Locations' distance from provided zipcode.
            visibility: str
                Either `PUBLIC` or `PRIVATE`. Private events only return if user is authenticated;
                if `visibility=PRIVATE` and user doesn't have permission, no events returned.
            exclude_full: bool
                If `exclude_full=true`, filter out full Timeslots (and Events if all of an Event's
                Timeslots are full)
            is_virtual: bool
                `is_virtual=false` will return only in-person events, while `is_virtual=true` will
                return only virtual events. If excluded, return virtual and in-person events. Note
                that providing a zipcode also implies `is_virtual=false`.
            event_types:enum
                The type of the event, one of: `CANVASS`, `PHONE_BANK`, `TEXT_BANK`, `MEETING`,
                `COMMUNITY`, `FUNDRAISER`, `MEET_GREET`, `HOUSE_PARTY`, `VOTER_REG`, `TRAINING`,
                `FRIEND_TO_FRIEND_OUTREACH`, `DEBATE_WATCH_PARTY`, `ADVOCACY_CALL`, `OTHER`.
                This list may expand in the future.
            max_timeslots: int
                If not returning a timeslot table, will unpack time slots. If do not
                set this arg, it will add a column for each time slot. The argument
                limits the number of columns and discards any additional timeslots
                after that.

                For example: If there are 20 timeslots associated with your event,
                and you set the max time slots to 5, it will only return the first 5
                time slots as ``time_slot_0``, ``time_slot_1`` etc.

                This is helpful in situations where you have a regular sync
                running and want to ensure that the column headers remain static.

        `Returns`
            Parsons Table or dict or Parsons Tables
                See :ref:`parsons-table` for output options.
        """

        valid_output_formats = ['Parsons', 'JSON']

        if output_format not in valid_output_formats:
            raise ValueError(f'Invalid output_format (must be one of: {self.output_formats}')

        if isinstance(organization_id, (str, int)):
            organization_id = [organization_id]

        args = {'organization_id': organization_id,
                'updated_since': date_to_timestamp(updated_since),
                'timeslot_start': self._time_parse(timeslot_start),
                'timeslot_end': self._time_parse(timeslot_end),
                'zipcode': zipcode}

        tbl = Table(self._request(self.uri + 'events', args=args, suppress_args_on_paginate=True))

        if tbl.num_rows > 0:

            tbl.unpack_dict('sponsor')
            tbl.unpack_dict('location', prepend=False)
            tbl.unpack_dict('location', prepend=False)  # Intentional duplicate
            tbl.table = petl.convert(tbl.table, 'address_lines', lambda v: ' '.join(v))

            if timeslots_table:

                timeslots_tbl = tbl.long_table(['id'], 'timeslots', {'id': 'event_id'})
                return {'events': tbl, 'timeslots': timeslots_tbl}

            elif unpack_timeslots:
                tbl.unpack_list('timeslots', replace=True, max_columns=max_timeslots)
                cols = tbl.columns
                for c in cols:
                    if re.search('timeslots', c, re.IGNORECASE) is not None:
                        tbl.unpack_dict(c)

        if output_format == 'Parsons':

            return tbl  # This is where the original method ends

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

    def get_events_auth(self, organization_id=None, updated_since=None, timeslot_start=None,
                                timeslot_end=None, timeslots_table=False, unpack_timeslots=True, max_timeslots=None, zipcode=None, max_dist=None, visibility=None,
                                exclude_full=False, is_virtual=None, event_types=None, output_format='Parsons'):
        """
        Fetch all public events for an organization. This includes both events owned
        by the organization (as indicated by the organization field on the event object)
        and events of other organizations promoted by this specified organization.

        .. note::
            API Key Required

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
            zipcode: str
                Filter by a Events' Locations' postal code. If present, returns Events
                sorted by distance from zipcode. If present, virtual events will not be returned.
            max_dist: str
                Filter Events' Locations' distance from provided zipcode.
            visibility: str
                Either `PUBLIC` or `PRIVATE`. Private events only return if user is authenticated;
                if `visibility=PRIVATE` and user doesn't have permission, no events returned.
            exclude_full: bool
                If `exclude_full=true`, filter out full Timeslots (and Events if all of an Event's
                Timeslots are full)
            is_virtual: bool
                `is_virtual=false` will return only in-person events, while `is_virtual=true` will
                return only virtual events. If excluded, return virtual and in-person events. Note
                that providing a zipcode also implies `is_virtual=false`.
            event_types:enum
                The type of the event, one of: `CANVASS`, `PHONE_BANK`, `TEXT_BANK`, `MEETING`,
                `COMMUNITY`, `FUNDRAISER`, `MEET_GREET`, `HOUSE_PARTY`, `VOTER_REG`, `TRAINING`,
                `FRIEND_TO_FRIEND_OUTREACH`, `DEBATE_WATCH_PARTY`, `ADVOCACY_CALL`, `OTHER`.
                This list may expand in the future.
            max_timeslots: int
                If not returning a timeslot table, will unpack time slots. If do not
                set this arg, it will add a column for each time slot. The argument
                limits the number of columns and discards any additional timeslots
                after that.

                For example: If there are 20 timeslots associated with your event,
                and you set the max time slots to 5, it will only return the first 5
                time slots as ``time_slot_0``, ``time_slot_1`` etc.

                This is helpful in situations where you have a regular sync
                running and want to ensure that the column headers remain static.

        `Returns`
            Parsons Table or dict or Parsons Tables
                See :ref:`parsons-table` for output options.
        """

        valid_output_formats = ['Parsons', 'JSON']

        if output_format not in valid_output_formats:
            raise ValueError(f'Invalid output_format (must be one of: {self.output_formats}')

        if isinstance(organization_id, (str, int)):
            organization_id = [organization_id]

        args = {'organization_id': organization_id,
                'updated_since': date_to_timestamp(updated_since),
                'timeslot_start': self._time_parse(timeslot_start),
                'timeslot_end': self._time_parse(timeslot_end),
                'zipcode': zipcode,
                'max_dist': max_dist,
                'visibility': visibility,
                'exclude_full': exclude_full,
                'is_virtual': is_virtual,
                'event_types': event_types
               }

        json_response = self._request(self.uri + 'events', args=args, suppress_args_on_paginate=True, auth=True)

        tbl = Table(json_response)

        if tbl.num_rows > 0:

            tbl.unpack_dict('sponsor')
            tbl.unpack_dict('location', prepend=False)
            tbl.unpack_dict('location', prepend=False)  # Intentional duplicate
            tbl.table = petl.convert(tbl.table, 'address_lines', lambda v: ' '.join(v))

            if timeslots_table:

                timeslots_tbl = tbl.long_table(['id'], 'timeslots', {'id':'event_id'})
                return {'events': tbl, 'timeslots': timeslots_tbl}

            elif unpack_timeslots:
                tbl.unpack_list('timeslots', replace=True, max_columns=max_timeslots)
                cols = tbl.columns
                for c in cols:
                    if re.search('timeslots', c, re.IGNORECASE) is not None:
                        tbl.unpack_dict(c)

        if output_format == 'Parsons':

            return tbl #This is where the original method ends

        elif output_format == 'JSON':
            #For the Airbyte integration, we need to output a JSON object
            #To get from the Parsons table object to a JSON, we can save the table as a csv, then use petl to extract the csv and load it into a json, which we then return

            #Write table to a CSV
            csv_filename = tbl.to_csv()

            #Extract the CSV into a new petl table
            testcsv = fromcsv(csv_filename)

            #Load the petl table into a json file
            tojson(testcsv, 'output.json') #convert the CSV to json

            #Load the json file into a JSON object
            f = open('output.json')
            json_output = json.load(f)

            return json_output