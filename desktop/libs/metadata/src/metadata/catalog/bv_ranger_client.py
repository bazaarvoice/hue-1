# For Ranger 0.7.0
from metadata import s3_utils
from metadata.catalog.ranger_client import RangerClient

class BvRangerClient(RangerClient):

    def __init__(self):
        super(BvRangerClient, self).__init__()


    """
        Returns a set of strings whose values are username strings.
        The strings are sourced from Ranger Policies GET endpt. 
        The results are filtered on the parameter 'policy_ids'.
        'policy_ids' is a list of integers, the value are policy ids for filtering policies
    """
    def get_bv_users(self, policy_ids):
        users = super(BvRangerClient, self).get_users(policy_ids)
        filtered_users = set()
        for user in users:
            if '@bazaarvoice.com' in user:
                filtered_users.add(user)
        return filtered_users