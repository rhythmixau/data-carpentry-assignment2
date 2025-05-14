
class Agent(object):
    def __init__(self,
                 agent_id: str,
                 full_name: str,
                 job_title: str,
                 email: str,
                 website: str,
                 phone_number: str,
                 mobile_number: str
                 ):
        self.agent_id = agent_id
        self.full_name = full_name
        self.job_title = job_title
        self.email = email
        self.website = website
        self.phone_number = phone_number
        self.mobile_number = mobile_number