import requests
import metadata.conf as conf

# For Ranger 0.7.0
class RangerClient(object):

    def __init__(self):
        self.host = conf.get_raven_ranger_host()
        self.port = conf.get_raven_ranger_port()
        self.username = conf.get_raven_ranger_username()
        self.password = conf.get_raven_ranger_password()

    """
        Returns users from Ranger Policies GET endpt filtered on ids
        policy_ids is a list of integers, where the value is the id of the policy
    """
    def get_users(self, policy_ids):
        #todo handle pagination
        users = set()
        policy_ids = set(policy_ids)
        url = "http://%s:%d/service/plugins/policies/service/1" % (self.host, self.port)
        resp = requests.get(url, auth=(self.username, self.password))
        j = resp.json()
        policies = j['policies'] # list of maps
        for policy in policies:
            if 'id' not in policy or policy['id'] not in policy_ids:
                continue
            policyItems = policy['policyItems'] # list of maps
            for policyItem in policyItems:
                policy_users = policyItem['users'] # list of strings
                policy_users = set(policy_users)
                users.update(policy_users)
        return users