import tableauserverclient as TSC
import logging


def refresh(user_name, user_pass, datasource, project):
    """
    This function allows you to refresh a Tableau Dashboard/Datasource via TSC
    :param user_name: TSC User
    :param user_pass: TSC Password
    :param datasource: Workbook/data source name
    :param project: Folder name where the workbook/datasource is
    :return: no return, if you need a return update the exit to be a variable that is returned to the calling fucntion
    """
    # Sets logging level to just errors
    logging.basicConfig(level=logging.ERROR)
    address = "" ##URL to Tableau
    server_version = ''  # 3.5 is for 2019.3.2, use what ever version you are on
    tableau_auth = TSC.TableauAuth(user_name, user_pass)  # Login command
    server = TSC.Server(address, server_version)  # The server info combined
    request_option = TSC.RequestOptions()  # Request Options mapped

    with server.auth.sign_in(tableau_auth):  # Login to the server using the above information
        # You need the workbook/Datasources ID to do anything, so this is how you filter down to it. I look for the name first
        request_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals,
                                             datasource))
        all_datasources, pagination_item = server.datasources.get(request_option)
        ds = next((ds for ds in all_datasources if ds.project_name == project), None)
        # if its not the project we specify, then None
        if ds is not None:  # If/Else
            resource = server.datasources.get_by_id(ds.id)  # finds the datasource using the ID
            server.datasources.refresh(resource)  # refreshes the datasource using the id we found via the name
            server.auth.sign_out()  # signs you out
            exit(0)  # exits correctly
        else:
            server.auth.sign_out()  # signs you out
            exit(1)  # exits because of an error
