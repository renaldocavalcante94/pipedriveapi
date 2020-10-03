import requests
import pandas as pd


class PipedriveAPI():
    
    object_list = ['activities','activityFields','activityTypes','callLogs','currencies','deals',
    'dealFields','files','filters','globalMessages','leads','notes','noteFields','organizationFields',
    'organizations','organizationRelationships','permissionSets','persons','personFields','pipelines',
    'products','productFields','roles','stages','teams','users','userConnections','webhooks']

    def __init__(self,company_domain,api_token):

        self.start_url = f"https://{company_domain}.pipedrive.com/api/v1/"
        self.api_token = api_token
        self.api_token_url = f"&api_token={self.api_token}"

    def get_all(self,object_name,as_df=True):
        """Get all items from an specific object in the pipedrive

        Args:
            object_name (str): the name of the object can be obtained in the self.object_list
            as_df (bool, optional): Parameter to determine if this method will return a 
            list of dicts or a pandas.Dataframe. Defaults to True.

        Returns:
            list or pandas.Dataframe:
        """
        
        start_index=0
        end_index=500

        x = self.get_object(object_name,start_index=start_index,end_index=end_index,as_df=False)

        while len(x) == end_index:
            start_index += 500
            end_index += 500
            
            y = self.get_object(object_name,start_index=start_index,end_index=end_index,as_df=False)
            
            if y is not None:
                x += y           
            

        if as_df:
            df = pd.DataFrame(x)
            return df
        else:
            return x



    def test_api(self):
        pass

    def get_object(self,object_name,start_index=None,end_index=None,as_df=True):
        """Get a pipedrive object given the parameters above

        Args:
            object_name (str): Object that will be requested in Pipedrive
            start_index (int, optional): Start index of the search. Defaults to None.
            end_index (int, optional): End index of the search. Defaults to None.
            as_df (bool, optional): If True, returns the result as a pandas.Dataframe 
            if False returns a list of dicts. Defaults to True.

        Returns:
            list or pandas.Dataframe: Returrns the object given the parameters, if as_df == True, 
            will returns a pandas.Dataframe else will be returned a list of dicts
        """

        if start_index != None and end_index != None:
            url = self._build_url(object_name,start_index,end_index)
        else:
            url = self.build_url(object_name)
        
        request = self._http_get_request(url)

        if as_df:
            df = self._request_json_to_df(request.json())

            return df
        else:
            json_return = request.json()['data']
            return json_return


    def _build_url(self,object_name,start_index=None,end_index=None):
        """Build an url with the given parameters

        Args:
            object_name (str): Pipedrive object name
            start_index (int, optional): Start Index in the PipedriveApi. Defaults to None.
            end_index (int, optional): End Index in the PipedriveApi. Defaults to None.

        Returns:
            str: Returns a url, to build the request
        """

        object_url = f"{object_name}?"
        
        if start_index != None and end_index != None:
            index_parameter_url = f"start={start_index}&limit={end_index}"
            url = self.start_url + object_url + index_parameter_url + self.api_token_url
            return url
        else:
            uirl = self.start_url + object_url + self.api_token_url
        

    def _request_json_to_df(self,json):
        """Convert a json from the Pipedrive API to pandas dataframe

        Args:
            json ([type]): [description]

        Returns:
            [type]: [description]
        """
        df = pd.DataFrame(json['data'])
        
        return df

    def _http_get_request(self,url):
        """
            Send a get Request for Pipefy API, folowwing the url
        Args:
            url (str): URL to request

        Raises:
            Exception: Expect with the error in case of any authentication error
            Exception: Raise an error in requests

        Returns:
            [request]: Returns the object requests
        """
        print(url)
        request = requests.get(url)

        status_code = request.status_code
        
        if status_code != 200:
            if status_code == 401:
                raise Exception(f"Authentication Error")
            raise Exception("Some error in th e request")

        return request

    
           


       



    