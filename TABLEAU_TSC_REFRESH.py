import tableauserverclient as TSC
import logging

"""
Name: Tableau Refresh                            
Created by: Chris Wilson, Sam McVicar                   
Creation Date: 11/9/2020                                
Purpose: Refreshes a tableau workbook/datasource via TSC API.                                                                             
"""

def refresh(user_name, user_pass, datasource, project):
    # Sets logging level to just errors
    logging.basicConfig(level=logging.ERROR)

    address = "HTTP://tableau"
    server_version = '3.5'  # This is for 2019.3.2
    tableau_auth = TSC.TableauAuth(user_name, user_pass)  # Login command
    server = TSC.Server(address, server_version)  # The server info combined
    request_option = TSC.RequestOptions()  # Request Options mapped

    with server.auth.sign_in(tableau_auth):  # Login to the server using the above information
        print('The connection has been made')

        # You need the workbook/Datasources ID to do anything, so this is how you filter down to it. I look for the name first
        request_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals,
                                             datasource))
        all_datasources, pagination_item = server.datasources.get(request_option)

        ds = next((ds for ds in all_datasources if ds.project_name == project), None)
        # if its not the project we specify, then None

        if ds is not None:  # If/Else

            resource = server.datasources.get_by_id(ds.id)  # finds the datasource using the ID
            server.datasources.refresh(resource)  # refreshes the datasource using the id we found via the name
            print('Data Source Refreshed, BEEP BOOP')
            server.auth.sign_out()  # signs you out
            print('The connection is closed')
            exit(0)  # exits correctly
        else:
            print('Invalid Details, Check Data Source or Folder Names... noob')
            server.auth.sign_out()  # signs you out
            print('The connection is closed')
            exit(1)  # exits because of an error
