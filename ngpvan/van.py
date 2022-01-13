import logging
from parsons_utilities.ngpvan.events import Events
from parsons_utilities.ngpvan.van_connector import VANConnector
from parsons_utilities.ngpvan.people import People
from parsons_utilities.ngpvan.saved_lists import SavedLists, Folders, ExportJobs
from parsons_utilities.ngpvan.activist_codes import ActivistCodes
from parsons_utilities.ngpvan.canvass_responses import CanvassResponses
from parsons_utilities.ngpvan.survey_questions import SurveyQuestions
from parsons_utilities.ngpvan.supporter_groups import SupporterGroups
from parsons_utilities.ngpvan.codes import Codes
from parsons_utilities.ngpvan.scores import Scores, FileLoadingJobs
from parsons_utilities.ngpvan.signups import Signups
from parsons_utilities.ngpvan.locations import Locations
from parsons_utilities.ngpvan.bulk_import import BulkImport
from parsons_utilities.ngpvan.changed_entities import ChangedEntities
from parsons_utilities.ngpvan.custom_fields import CustomFields
from parsons_utilities.ngpvan.targets import Targets

logger = logging.getLogger(__name__)


class VAN(People, Events, SavedLists, Folders, ExportJobs, ActivistCodes, CanvassResponses,
          SurveyQuestions, Codes, Scores, FileLoadingJobs, SupporterGroups, Signups, Locations,
          BulkImport, ChangedEntities, CustomFields, Targets):
    """
    Returns the VAN class

    `Args:`
        api_key : str
            A valid api key Not required if ``VAN_API_KEY`` env variable set.
        auth_name: str
            Should not pass this argument
        db: str
            One of ``MyVoters``, ``MyMembers``, ``MyCampaign``, or ``EveryAction``
        uri: str
            Base uri to make api calls.
        raise_for_status: boolean
            Raise excection when encountering a 4XX or 500 error.
    `Returns:`
        VAN object
    """

    def __init__(self, api_key=None, auth_name='default', db=None, raise_for_status=True):

        self.connection = VANConnector(api_key=api_key, db=db)
        self.api_key = api_key
        self.db = db

        # The size of each page to return. Currently set to maximum.
        self.page_size = 200
