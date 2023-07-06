import requests

def clear_cache():

    url  = 'http://10.40.16.19:8080/pentaho/plugin/cda/api/clearCache?file=branch_incentive.cda'

    auth = ('biadmin', '@MAktub/..')
    r = requests.get(url,auth=auth)

    if r.status_code == 200:
        print("Cache cleared successfully")
    else:
        print("Error clearing cache. Error code: ", r.status_code)
